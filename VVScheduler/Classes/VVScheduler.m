//
//  VVScheduler.m
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import "VVScheduler.h"
#import <pthread/pthread.h>
#import  "VVThreadMutableArray.h"
#import  "VVThreadMutableDictionary.h"

@interface NSThread (VVScheduler)

+ (NSThread *)threadWithName:(NSString *)name qualityOfService:(NSQualityOfService)qualityOfService;

@end

@implementation NSThread (VVScheduler)

+ (void)_schedulerThreadMan:(NSString *)name
{
    @autoreleasepool {
        [[NSThread currentThread] setName:name];
        NSRunLoop *runLoop = [NSRunLoop currentRunLoop];
        [runLoop addPort:[NSMachPort port] forMode:NSDefaultRunLoopMode];
        [runLoop run];
    }
}

+ (NSThread *)threadWithName:(NSString *)name qualityOfService:(NSQualityOfService)qualityOfService
{
    NSThread *thread = [[NSThread alloc] initWithTarget:self selector:@selector(_schedulerThreadMan:) object:name];
    thread.qualityOfService = qualityOfService;
    [thread start];
    return thread;
}

@end

static dispatch_queue_t dispatch_create_scheduler_queue(const char *_Nullable tag, dispatch_queue_attr_t _Nullable attr)
{
    static NSUInteger i = 0;
    NSString *label = [NSString stringWithFormat:@"com.enigma.scheduler.%s.%@", tag ? : "default", @(i++)];
    return dispatch_queue_create(label.UTF8String, attr);
}

@interface VVSchedulerItemExtra : NSObject
@property (nonatomic, assign) CFAbsoluteTime suspendTime;
@property (nonatomic, assign) CFAbsoluteTime updateAt;
@property (nonatomic, assign) float lastProgress;
@end

@implementation VVSchedulerItemExtra
@end

@interface VVScheduler ()
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, assign) BOOL active;
@property (nonatomic, strong) VVThreadMutableArray<VVSchedulerItem> *runningTasks;
@property (nonatomic, strong) VVThreadMutableArray<VVSchedulerItem> *suspendTasks;
@property (nonatomic, strong) VVThreadMutableDictionary<VVSchedulerKey, VVSchedulerItem> *allTasks;
@property (nonatomic, strong) VVThreadMutableDictionary<VVSchedulerKey, VVSchedulerItemExtra *> *allExtras;
@property (nonatomic, strong) VVMergeValve *mergeValve;
@property (nonatomic, strong) VVLastValve *lastValve;
@property (nonatomic, strong) dispatch_queue_t queue;
@end

@implementation VVScheduler

+ (instancetype)schedulerWithPolicy:(VVSchedulerPolicy)policy
{
    return [[VVScheduler alloc] initWithPolicy:policy];
}

- (instancetype)initWithPolicy:(VVSchedulerPolicy)policy
{
    self = [super init];
    if (self) {
        _maxActivations = 8;
        _timeout = 60;
        _durationOfSuspension = 30;
        _interval = 1.0;
        _policy = policy;
        _autoPoll = NO;
        _runningTasks = [VVThreadMutableArray array];
        _suspendTasks = [VVThreadMutableArray array];
        _allTasks = [VVThreadMutableDictionary dictionary];
        _allExtras = [VVThreadMutableDictionary dictionary];
        _queue = dispatch_create_scheduler_queue(NULL, DISPATCH_QUEUE_CONCURRENT);
        [self setupTimer];
        [self setupMergeValve];
        [self setupLastValve];
    }
    return self;
}

- (void)setupMergeValve
{
    _mergeValve = [[VVMergeValve alloc] init];
    __weak typeof(self) weakself = self;
    [_mergeValve setMergeAction:^(NSArray *objects, NSDictionary *keyObjects) {
        __strong typeof(weakself) strongself = weakself;
        if (!strongself) return;
        //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, @(objects.count));
        dispatch_async(strongself.queue, ^{ [strongself poll]; });
    }];
}

- (void)setupLastValve
{
    _lastValve = [[VVLastValve alloc] init];
    _lastValve.interval = 10.0;
}

- (void)dealloc
{
    if (!_active) dispatch_resume(_timer);
    dispatch_cancel(_timer);
    _timer = nil;
}

- (void)setupTimer
{
    _timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, self.queue);
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
    __weak typeof(self) weakself = self;
    dispatch_source_set_event_handler(_timer, ^{
        __strong typeof(weakself) strongself = weakself;
        [strongself poll];
    });
}

- (void)setAutoPoll:(BOOL)autoPoll
{
    _autoPoll = autoPoll;
    if (autoPoll) {
        dispatch_resume(self.timer);
        _active = YES;
    } else {
        dispatch_suspend(self.timer);
        _active = NO;
    }
}

- (void)manualPoll
{
    [self.mergeValve addObject:@(0) forKey:@(0)];
}

- (void)delayPoll
{
    if (self.runningTasks.count == 0) return;
    //NSLog(@"#-> %p: %s", self, __PRETTY_FUNCTION__);

    __weak typeof(self) weakself = self;
    [self.lastValve doAction:^{
        __strong typeof(weakself) strongself = weakself;
        if (!strongself) {
            return;
        }
        [strongself manualPoll];
    }];
}

- (void)poll
{
    if (_runningTasks.count == 0) {
        return;
    }
    //MARK: VVSchedulerPolicyPriority not supported
    /*
     if (_policy == VVSchedulerPolicyPriority) {
         [_runningTasks sortUsingComparator:^NSComparisonResult (VVSchedulerItem task1, VVSchedulerItem task2) {
            return task1.priority > task2.priority ? NSOrderedAscending : NSOrderedDescending;
         }];
     }
     */
    //NSLog(@"#-> %p: %s [0] queue:%@,suspend:%@", self, __PRETTY_FUNCTION__, @(_runningTasks.count), @(_suspendTasks.count));
    // running
    VVThreadMutableArray *completedTasks = [VVThreadMutableArray array];
    VVThreadMutableArray *completedIds = [VVThreadMutableArray array];
    CFAbsoluteTime now = CFAbsoluteTimeGetCurrent();
    NSUInteger running = 0, pause = 0, resume = 0;
    NSArray *runningTasks = [NSArray arrayWithArray:_runningTasks];
    for (VVSchedulerItem task in runningTasks) {
        VVSchedulerItemExtra *extra = _allExtras[task.identifier];
        if (!extra) {
            extra = [VVSchedulerItemExtra new];
            _allExtras[task.identifier] = extra;
        }
        switch (task.state) {
            case VVSchedulerTaskStateRunning: {
                float fractionCompleted = task.progress.fractionCompleted;
                BOOL delay = fractionCompleted <= extra.lastProgress && now - extra.updateAt > _timeout;
                if (running >= _maxActivations || delay) {
                    if (delay) extra.suspendTime = now;
                    dispatch_async(self.queue, ^{ [task suspend]; });
                    pause++;
                } else {
                    extra.updateAt = now;
                    running++;
                }
                extra.lastProgress = fractionCompleted;
                break;
            }
            case VVSchedulerTaskStateSuspended: {
                BOOL wakeup = extra.suspendTime == 0 || (now - extra.suspendTime > _durationOfSuspension);
                if (running < _maxActivations && wakeup) {
                    dispatch_async(self.queue, ^{ [task resume]; });
                    extra.updateAt = now;
                    extra.lastProgress = task.progress.fractionCompleted;
                    extra.suspendTime = 0;
                    running++;
                    resume++;
                }
                break;
            }
            case VVSchedulerTaskStateCanceling:
            case VVSchedulerTaskStateCompleted: {
                [completedTasks addObject:task];
                [completedIds addObject:task.identifier];
                break;
            }
            default: break;
        }
    }

    [_runningTasks removeObjectsInArray:completedTasks];
    [_allTasks removeObjectsForKeys:completedIds];
    [_allExtras removeObjectsForKeys:completedIds];
    //NSLog(@"#-> %p: %s [1] queue:%@,suspend:%@,run:%@,pause:%@,resume:%@,completed:%@", self, __PRETTY_FUNCTION__, @(_runningTasks.count), @(_suspendTasks.count), @(running), @(pause), @(resume), @(completedTasks.count));
    [completedTasks removeAllObjects];
    [completedIds removeAllObjects];
    if (!_autoPoll) {
        [self delayPoll];
    }
}

- (void)addTask:(VVSchedulerItem)task
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, task);
    VVSchedulerItem newTask = task;
    VVSchedulerItem oldTask = [_allTasks objectForKey:task.identifier];
    if (oldTask) {
        if (![oldTask isEqual:task]) {
            if (oldTask.state < VVSchedulerTaskStateCanceling) {
                dispatch_async(self.queue, ^{ [oldTask cancel]; });
            }
            [_runningTasks removeObject:oldTask];
            [_suspendTasks removeObject:oldTask];
            [_allTasks removeObjectForKey:task.identifier];
            //NSLog(@"#-> remove old task: %@", taskId);
        } else {
            newTask = oldTask;
        }
    }

    [_allTasks setObject:task forKey:task.identifier];
    [_runningTasks removeObject:newTask];
    [_suspendTasks removeObject:newTask];
    if (_policy == VVSchedulerPolicyLIFO) {
        [_runningTasks insertObject:task atIndex:0];
    } else {
        [_runningTasks addObject:task];
    }
    //NSLog(@"#-> add task: %@", taskId);
}

- (void)addTasks:(NSArray<VVSchedulerItem> *)tasks
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, tasks);
    VVThreadMutableArray *removeTasks = [VVThreadMutableArray array];
    VVThreadMutableArray *appendTasks = [VVThreadMutableArray array];
    VVThreadMutableArray *removeKeys = [VVThreadMutableArray array];
    VVThreadMutableDictionary *apppendKeyItems = [VVThreadMutableDictionary dictionary];
    for (VVSchedulerItem task in tasks) {
        VVSchedulerItem oldTask = [_allTasks objectForKey:task.identifier];
        VVSchedulerItem newTask = task;
        if (oldTask) {
            [removeTasks addObject:oldTask];
            [removeKeys addObject:oldTask.identifier];
            if (![oldTask isEqual:task]) {
                if (oldTask.state < VVSchedulerTaskStateCanceling) {
                    dispatch_async(self.queue, ^{ [oldTask cancel]; });
                }
            } else {
                newTask = oldTask;
            }
        }
        [appendTasks addObject:newTask];
        [apppendKeyItems setObject:newTask forKey:task.identifier];
    }

    [_runningTasks removeObjectsInArray:removeTasks];
    [_suspendTasks removeObjectsInArray:removeTasks];
    [_allTasks removeObjectsForKeys:removeKeys];

    [_allTasks addEntriesFromDictionary:apppendKeyItems];
    if (_policy == VVSchedulerPolicyLIFO) {
        NSIndexSet *indexSet = [NSIndexSet indexSetWithIndexesInRange:NSMakeRange(0, appendTasks.count)];
        [_runningTasks insertObjects:appendTasks atIndexes:indexSet];
    } else {
        [_runningTasks addObjectsFromArray:appendTasks];
    }
    _mergeValve.interval = MIN(1.0, MAX(0.1, (_runningTasks.count / 100) * 0.1));
}

- (void)suspendTask:(VVSchedulerKey)taskId
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskId);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        if ([_runningTasks containsObject:task]) {
            dispatch_async(self.queue, ^{ [task suspend]; });
        }
        [_runningTasks removeObject:task];
        [_suspendTasks addObject:task];
    }
}

- (void)suspendTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskIds);
    VVThreadMutableArray *changes = [VVThreadMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (!task) continue;
        if (task.state == VVSchedulerTaskStateRunning) {
            dispatch_async(self.queue, ^{ [task suspend]; });
        }
        [changes addObject:task];
    }
    [_runningTasks removeObjectsInArray:changes];
    [_suspendTasks addObjectsFromArray:changes];
}

- (void)resumeTask:(VVSchedulerKey)taskId
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskId);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task && [_suspendTasks containsObject:task]) {
        [_suspendTasks removeObject:task];
        [_runningTasks addObject:task];
    }
}

- (void)resumeTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskIds);
    VVThreadMutableArray *changes = [VVThreadMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (task && [_suspendTasks containsObject:task]) {
            [changes addObject:task];
        }
    }
    [_suspendTasks removeObjectsInArray:changes];
    [_runningTasks addObjectsFromArray:changes];
}

- (void)removeTask:(VVSchedulerKey)taskId
{
    [self removeTask:taskId cancel:NO];
}

- (void)cancelAndRemoveTask:(VVSchedulerKey)taskId
{
    [self removeTask:taskId cancel:YES];
}

- (void)removeTask:(VVSchedulerKey)taskId cancel:(BOOL)cancel
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskId);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        if (cancel && task.state < VVSchedulerTaskStateCanceling) {
            dispatch_async(self.queue, ^{ [task cancel]; });
        }
        [_runningTasks removeObject:task];
        [_suspendTasks removeObject:task];
        [_allTasks removeObjectForKey:taskId];
        [_allExtras removeObjectForKey:taskId];
    }
}

- (void)cancelAndRemoveTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskIds);
    VVThreadMutableArray *changes = [VVThreadMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (!task) continue;
        if (task.state < VVSchedulerTaskStateCanceling) {
            dispatch_async(self.queue, ^{ [task cancel]; });
        }
        [changes addObject:task];
    }
    [_runningTasks removeObjectsInArray:changes];
    [_suspendTasks removeObjectsInArray:changes];
    [_allTasks removeObjectsForKeys:taskIds];
    [_allExtras removeObjectsForKeys:taskIds];
}

- (void)cancelAndRemoveAllTasks
{
    //NSLog(@"#-> %p: %s", self, __PRETTY_FUNCTION__);
    for (VVSchedulerItem task in _allTasks.allValues) {
        if (task.state < VVSchedulerTaskStateCanceling) {
            dispatch_async(self.queue, ^{ [task cancel]; });
        }
    }
    [_runningTasks removeAllObjects];
    [_suspendTasks removeAllObjects];
    [_allTasks removeAllObjects];
    [_allExtras removeAllObjects];
}

- (void)prioritizeTask:(VVSchedulerKey)taskId {
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskId);
    if (!taskId) return;

    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        //NSLog(@"#-> prioritize task: %@", taskId);
        [_suspendTasks removeObject:task];
        [_runningTasks removeObject:task];
        [_runningTasks insertObject:task atIndex:0];

        //MARK: VVSchedulerPolicyPriority not supported
        /*
         if (_policy == VVSchedulerPolicyPriority) {
         float maxPriority = 0;
         for (VVSchedulerItem task in _runningTasks) {
         maxPriority = MAX(maxPriority, task.priority);
         }
         task.priority = maxPriority + (1 - maxPriority) / (_runningTasks.count * 1.0);
         }
         */
    }
}

- (void)prioritizeTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, taskIds);
    VVThreadMutableArray *changes = [VVThreadMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (!task) continue;
        [changes addObject:task];
    }
    [_runningTasks removeObjectsInArray:changes];
    [_suspendTasks removeObjectsInArray:changes];
    NSIndexSet *indexSet = [NSIndexSet indexSetWithIndexesInRange:NSMakeRange(0, changes.count)];
    [_runningTasks insertObjects:changes atIndexes:indexSet];
}

- (nullable id<VVSchedulerTask>)objectForKeyedSubscript:(VVSchedulerKey)key
{
    VVSchedulerItem task = [_allTasks objectForKey:key];
    return task;
}

- (void)setObject:(nullable id<VVSchedulerTask>)obj forKeyedSubscript:(VVSchedulerKey)key
{
    if (obj) {
        [self addTask:obj];
    } else {
        [self removeTask:key];
    }
}

@end

@interface VVMergeValve ()
@property (nonatomic, strong) VVThreadMutableArray *objects;
@property (nonatomic, strong) VVThreadMutableDictionary *keyObjects;
@property (nonatomic, strong) dispatch_queue_t queue;
@end

@implementation VVMergeValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _interval = 0.1;
        _maxMergeCount = 100;
        _objects = [VVThreadMutableArray array];
        _keyObjects = [VVThreadMutableDictionary dictionary];
        _queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    }
    return self;
}

- (void)addObject:(id)object forKey:(id<NSCopying>)key
{
    //NSLog(@"#-> %p: %s %@ %@", self, __PRETTY_FUNCTION__, key, object);
    if (!key) return;

    if (self.keyObjects.count == 0) {
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(self.interval * NSEC_PER_SEC)), self.queue, ^{
            [self runMergeOpreation];
        });
    }
    id oldObj = self.keyObjects[key];
    if (oldObj) {
        [self.objects removeObject:oldObj];
    }
    self.keyObjects[key] = object;
    if (object) {
        [self.objects insertObject:object atIndex:0];
    }
    if (self.objects.count >= self.maxMergeCount) {
        [self runMergeOpreation];
    }
}

- (void)runMergeOpreation
{
    @synchronized (self) {
        //NSLog(@"#-> %p: %s, %@", self, __PRETTY_FUNCTION__, @(self.objects.count));
        if (!self.mergeAction) return;
        NSArray *objects = [NSArray arrayWithArray:self.objects];
        NSDictionary *keyObjects = [NSDictionary dictionaryWithDictionary:self.keyObjects];
        [self.objects removeAllObjects];
        [self.keyObjects removeAllObjects];
        self.mergeAction(objects, keyObjects);
    }
}

@end

@interface VVLimitValve ()
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, strong) VVThreadMutableArray *objects;
@property (nonatomic, assign) BOOL active;
@end

@implementation VVLimitValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _objects = [VVThreadMutableArray array];
        _interval = 0.1;
        [self setupTimer];
    }
    return self;
}

- (void)dealloc
{
    if (!_active) dispatch_resume(_timer);
    dispatch_cancel(_timer);
    _timer = nil;
}

- (void)setupTimer
{
    dispatch_queue_t queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    _timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, queue);
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
    __weak typeof(self) weakself = self;
    dispatch_source_set_event_handler(_timer, ^{
        __strong typeof(weakself) strongself = weakself;
        [strongself poll];
    });
}

- (void)setInterval:(NSTimeInterval)interval
{
    _interval = interval;
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
}

- (void)addObject:(id)object
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, object);
    if (self.objects.count == 0 && !_active) {
        dispatch_resume(self.timer);
        _active = YES;
    }
    [self.objects addObject:object];
}

- (void)poll
{
    if (self.objects.count > 0) {
        id object = self.objects.firstObject;
        [self.objects removeObjectAtIndex:0];
        if (self.objects.count == 0 && _active) {
            dispatch_suspend(self.timer);
            _active = NO;
        }
        if (self.takeAction) self.takeAction(object);
    }
}

@end

@interface VVLastValve ()
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, assign) BOOL active;
@property (nonatomic, copy) void (^ action)(void);
@end

@implementation VVLastValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _interval = 0.3;
        [self setupTimer];
    }
    return self;
}

- (void)setupTimer
{
    dispatch_queue_t queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    _timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, queue);
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
    __weak typeof(self) weakself = self;
    dispatch_source_set_event_handler(_timer, ^{
        __strong typeof(weakself) strongself = weakself;
        [strongself execute];
    });
}

- (void)setInterval:(NSTimeInterval)interval
{
    _interval = interval;
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
}

- (void)doAction:(void (^)(void))action
{
    self.action = action;
    //NSLog(@"#-> %p[%@]: %s", self, @(_interval), __PRETTY_FUNCTION__);
    if (!_active) {
        dispatch_resume(_timer);
        _active = YES;
    }
}

- (void)execute
{
    if (!_active) return;

    //NSLog(@"#-> %p[%@]: %s", self, @(_interval), __PRETTY_FUNCTION__);
    !self.action ? : self.action();
    dispatch_suspend(_timer);
    _active = NO;
}

- (void)dealloc
{
    if (!_active) dispatch_resume(_timer);
    dispatch_cancel(_timer);
    _timer = nil;
}

@end
