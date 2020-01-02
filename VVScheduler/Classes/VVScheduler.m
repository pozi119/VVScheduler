//
//  VVScheduler.m
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import "VVScheduler.h"
#import "VVThreadMutableArray.h"
#import "VVThreadMutableDictionary.h"

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
@property (nonatomic, strong) VVThreadMutableArray<VVSchedulerItem> *runningTasks;
@property (nonatomic, strong) VVThreadMutableArray<VVSchedulerItem> *suspendTasks;
@property (nonatomic, strong) VVThreadMutableDictionary<VVSchedulerKey, VVSchedulerItem> *allTasks;
@property (nonatomic, strong) VVThreadMutableDictionary<VVSchedulerKey, VVSchedulerItemExtra *> *allExtras;
@property (nonatomic, strong) VVMergeValve *mergeValve;
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
    }
    return self;
}

- (void)setupMergeValve
{
    _mergeValve = [[VVMergeValve alloc] init];
    __weak typeof(self) weakSelf = self;
    [_mergeValve setMergeAction:^(NSArray *objects, NSDictionary *keyObjects) {
        __strong typeof(weakSelf) strongSelf = weakSelf;
        //NSLog(@"#-> %p: %s %@", strongSelf, __PRETTY_FUNCTION__, @(objects.count));
        dispatch_async(strongSelf.queue, ^{
            [strongSelf poll];
        });
    }];
}

- (void)dealloc
{
    dispatch_cancel(_timer);
    _timer = nil;
}

- (void)setupTimer
{
    _timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, self.queue);
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
    dispatch_source_set_event_handler(_timer, ^{
        [self poll];
    });
}

- (void)setAutoPoll:(BOOL)autoPoll
{
    _autoPoll = autoPoll;
    if (autoPoll) {
        dispatch_resume(self.timer);
    } else {
        dispatch_suspend(self.timer);
    }
}

- (void)manualPoll
{
    static NSUInteger i = 0;
    i++;
    [self.mergeValve addObject:@(i) forKey:@(i)];
}

- (void)delayPoll
{
    //NSLog(@"#-> %p: %s", self, __PRETTY_FUNCTION__);
    [[self class] cancelPreviousPerformRequestsWithTarget:self selector:@selector(_delayPoll) object:nil];
    [self performSelector:@selector(_delayPoll) withObject:nil afterDelay:10];
}

- (void)_delayPoll
{
    //NSLog(@"#-> %p: %s", self, __PRETTY_FUNCTION__);
    [self manualPoll];
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
    NSMutableArray *completedTasks = [NSMutableArray array];
    NSMutableArray *completedIds = [NSMutableArray array];
    CFAbsoluteTime now = CFAbsoluteTimeGetCurrent();
    NSUInteger running = 0, pause = 0, resume = 0;
    for (VVSchedulerItem task in _runningTasks) {
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
        dispatch_async(self.queue, ^{ [self delayPoll]; });
        //[self performSelector:@selector(delayPoll) onThread:self.thread withObject:nil waitUntilDone:NO];
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
}

- (void)addTasks:(NSArray<VVSchedulerItem> *)tasks
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, tasks);
    NSMutableArray *removeTasks = [NSMutableArray array];
    NSMutableArray *appendTasks = [NSMutableArray array];
    NSMutableArray *removeKeys = [NSMutableArray array];
    NSMutableDictionary *apppendKeyItems = [NSMutableDictionary dictionary];
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
    NSMutableArray *changes = [NSMutableArray array];
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
    NSMutableArray *changes = [NSMutableArray array];
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
    NSMutableArray *changes = [NSMutableArray array];
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
    NSMutableArray *changes = [NSMutableArray array];
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
        _queue = dispatch_create_scheduler_queue("merge", DISPATCH_QUEUE_SERIAL);
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
    if (!self.mergeAction) return;
    NSArray *objects = self.objects.copy;
    NSDictionary *keyObjects = self.keyObjects.copy;
    [self.objects removeAllObjects];
    [self.keyObjects removeAllObjects];
    self.mergeAction(objects, keyObjects);
}

@end

@interface VVLimitValve ()
@property (nonatomic, strong) dispatch_queue_t queue;
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, strong) VVThreadMutableArray *objects;
@property (nonatomic, assign) BOOL running;
@end

@implementation VVLimitValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _objects = [VVThreadMutableArray array];
        _queue = dispatch_create_scheduler_queue("limit", DISPATCH_QUEUE_SERIAL);
        [self setupTimer];
    }
    return self;
}

- (void)dealloc
{
    dispatch_cancel(_timer);
    _timer = nil;
}

- (void)setupTimer
{
    _timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, _queue);
    dispatch_source_set_timer(_timer, dispatch_time(DISPATCH_TIME_NOW, _interval * NSEC_PER_SEC), _interval * NSEC_PER_SEC, 0.1 * NSEC_PER_SEC);
    dispatch_source_set_event_handler(_timer, ^{
        [self poll];
    });
}

- (void)addObject:(id)object
{
    //NSLog(@"#-> %p: %s %@", self, __PRETTY_FUNCTION__, object);
    if (self.objects.count == 0 && !_running) {
        _running = YES;
        dispatch_resume(self.timer);
    }
    [self.objects addObject:object];
}

- (void)poll
{
    if (self.objects.count > 0) {
        id object = self.objects.firstObject;
        [self.objects removeObjectAtIndex:0];
        if (self.objects.count == 0 && _running) {
            dispatch_suspend(self.timer);
            _running = NO;
        }
        if (self.takeAction) self.takeAction(object);
    }
}

@end

@interface _VVLastValve : NSObject
@property (nonatomic, assign) NSTimeInterval interval;
@property (nonatomic, copy) void (^ action)(void);
@end

@implementation _VVLastValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _interval = 0.2;
    }
    return self;
}

- (void)doAction:(void (^)(void))action {
    self.action = action;
    [[self class] cancelPreviousPerformRequestsWithTarget:self selector:@selector(execute) object:nil];
    [self performSelector:@selector(execute) withObject:nil afterDelay:_interval];
}

- (void)execute {
    //NSLog(@"#-> %p: %s", self, __PRETTY_FUNCTION__);
    !self.action ? : self.action();
}

@end

@implementation VVLastValve

+ (NSMutableDictionary<id<NSCopying>, _VVLastValve *> *)valves
{
    static NSMutableDictionary *_valves;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        _valves = [NSMutableDictionary dictionary];
    });
    return _valves;
}

+ (_VVLastValve *)valveForIdentifier:(id<NSCopying>)identifier
{
    _VVLastValve *valve = [[self valves] objectForKey:identifier];
    if (!valve) {
        valve = [[_VVLastValve alloc] init];
        [[self valves] setObject:valve forKey:identifier];
    }
    return valve;
}

+ (void)setInterval:(NSTimeInterval)interval forIdenitifer:(id<NSCopying>)identifier
{
    _VVLastValve *valve = [self valveForIdentifier:identifier];
    valve.interval = interval;
}

+ (void)doAction:(nonnull void (^)(void))action forIdenitifer:(id<NSCopying>)identifier
{
    _VVLastValve *valve = [self valveForIdentifier:identifier];
    [valve doAction:action];
}

@end
