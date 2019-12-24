//
//  VVScheduler.m
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import "VVScheduler.h"

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

static NSThread * dispatch_create_scheduler_thread(NSString *_Nullable tag, NSQualityOfService qos)
{
    static NSUInteger i = 0;
    NSString *name = [NSString stringWithFormat:@"com.enigma.scheduler.%@.%@", tag ? : @"thread", @(i++)];
    return [NSThread threadWithName:name qualityOfService:qos];
}

@interface VVSchedulerItemExtra : NSObject
@property (nonatomic, assign) CFAbsoluteTime suspendTime;
@property (nonatomic, assign) CFAbsoluteTime lastUpdateTime;
@property (nonatomic, assign) float lastFractionCompleted;
@end

@implementation VVSchedulerItemExtra
@end

@interface VVScheduler ()
@property (nonatomic, assign) VVSchedulerPolicy policy;
@property (nonatomic, strong) dispatch_semaphore_t lock;
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, strong) NSMutableArray<VVSchedulerItem> *runningTasks;
@property (nonatomic, strong) NSMutableArray<VVSchedulerItem> *suspendTasks;
@property (nonatomic, strong) NSMutableDictionary<VVSchedulerKey, VVSchedulerItem> *allTasks;
@property (nonatomic, strong) NSMutableDictionary<VVSchedulerKey, VVSchedulerItemExtra *> *allExtras;
@property (nonatomic, strong) VVMergeValve *mergeValve;
@property (nonatomic, strong) dispatch_queue_t queue;
@property (nonatomic, strong) NSThread *thread;
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
        _runningTasks = [NSMutableArray array];
        _suspendTasks = [NSMutableArray array];
        _allTasks = [NSMutableDictionary dictionary];
        _allExtras = [NSMutableDictionary dictionary];
        _lock = dispatch_semaphore_create(1);
        _queue = dispatch_create_scheduler_queue(NULL, DISPATCH_QUEUE_CONCURRENT);
        _thread = dispatch_create_scheduler_thread(nil, NSQualityOfServiceUserInitiated);
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
        //NSLog(@"---> merge scheduler: %@, %@", @(objects.count), @(CFAbsoluteTimeGetCurrent()));
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
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    _autoPoll = autoPoll;
    if (autoPoll) {
        dispatch_resume(self.timer);
    } else {
        dispatch_suspend(self.timer);
    }
    dispatch_semaphore_signal(_lock);
}

- (void)manualPoll
{
    static NSUInteger i = 0;
    i++;
    [self.mergeValve addObject:@(i) forKey:@(i)];
}

- (void)delayPoll
{
    //NSLog(@"---> delayPoll");
    [[self class] cancelPreviousPerformRequestsWithTarget:self selector:@selector(_delayPoll) object:nil];
    [self performSelector:@selector(_delayPoll) withObject:nil afterDelay:10];
}

- (void)_delayPoll
{
    //NSLog(@"---> _delayPoll");
    [self manualPoll];
}

- (void)poll
{
    if (_runningTasks.count == 0) {
        return;
    }
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    //MARK: VVSchedulerPolicyPriority not supported
    /*
    if (_policy == VVSchedulerPolicyPriority) {
        [_runningTasks sortUsingComparator:^NSComparisonResult (VVSchedulerItem task1, VVSchedulerItem task2) {
            return task1.priority > task2.priority ? NSOrderedAscending : NSOrderedDescending;
        }];
    }
     */
    //NSLog(@"---> [0]queue:%@,suspend:%@", @(_runningTasks.count), @(_suspendTasks.count));
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
                if (running < _maxActivations) {
                    if (fractionCompleted <= extra.lastFractionCompleted && now - extra.lastUpdateTime > _timeout) {
                        extra.suspendTime = now;
                        [(NSObject *)task performSelector:@selector(suspend) onThread:self.thread withObject:nil waitUntilDone:NO];
                        //NSLog(@"---> delay: %@", task);
                    } else {
                        running++;
                    }
                } else {
                    extra.lastUpdateTime = now;
                    [(NSObject *)task performSelector:@selector(suspend) onThread:self.thread withObject:nil waitUntilDone:NO];
                    pause++;
                }
                extra.lastFractionCompleted = fractionCompleted;
                break;
            }
            case VVSchedulerTaskStateSuspended: {
                if (running < _maxActivations && (extra.suspendTime == 0 || (now - extra.suspendTime > _durationOfSuspension))) {
                    [(NSObject *)task performSelector:@selector(resume) onThread:self.thread withObject:nil waitUntilDone:NO];
                    //NSLog(@"---> resume: %@", task);
                    extra.lastUpdateTime = now;
                    extra.lastFractionCompleted = task.progress.fractionCompleted;
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
    //NSLog(@"---> [2]queue:%@,suspend:%@,run:%@,pause:%@,resume:%@,completed:%@", @(_runningTasks.count), @(_suspendTasks.count), @(running), @(pause), @(resume), @(completedTasks.count));
    [completedTasks removeAllObjects];
    [completedIds removeAllObjects];
    if (!_autoPoll) {
        [self performSelector:@selector(delayPoll) onThread:self.thread withObject:nil waitUntilDone:NO];
    }
    dispatch_semaphore_signal(_lock);
}

- (void)addTask:(VVSchedulerItem)task
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem newTask = task;
    VVSchedulerItem oldTask = [_allTasks objectForKey:task.identifier];
    if (oldTask) {
        if (![oldTask isEqual:task]) {
            if (oldTask.state < VVSchedulerTaskStateCanceling) {
                [(NSObject *)oldTask performSelector:@selector(cancel) onThread:self.thread withObject:nil waitUntilDone:NO];
            }
            [_runningTasks removeObject:oldTask];
            [_suspendTasks removeObject:oldTask];
            [_allTasks removeObjectForKey:task.identifier];
            //NSLog(@"---> remove old task: %@", taskId);
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
    //NSLog(@"---> add task: %@", taskId);
    dispatch_semaphore_signal(_lock);
}

- (void)addTasks:(NSArray<VVSchedulerItem> *)tasks
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
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
                    [(NSObject *)oldTask performSelector:@selector(cancel) onThread:self.thread withObject:nil waitUntilDone:NO];
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
    dispatch_semaphore_signal(_lock);
}

- (void)suspendTask:(VVSchedulerKey)taskId
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        if ([_runningTasks containsObject:task]) {
            [(NSObject *)task performSelector:@selector(suspend) onThread:self.thread withObject:nil waitUntilDone:NO];
        }
        [_runningTasks removeObject:task];
        [_suspendTasks addObject:task];
    }
    dispatch_semaphore_signal(_lock);
}

- (void)suspendTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    NSMutableArray *changes = [NSMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (!task) continue;
        if (task.state == VVSchedulerTaskStateRunning) {
            [(NSObject *)task performSelector:@selector(suspend) onThread:self.thread withObject:nil waitUntilDone:NO];
        }
        [changes addObject:task];
    }
    [_runningTasks removeObjectsInArray:changes];
    [_suspendTasks addObjectsFromArray:changes];
    dispatch_semaphore_signal(_lock);
}

- (void)resumeTask:(VVSchedulerKey)taskId
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task && [_suspendTasks containsObject:task]) {
        [_suspendTasks removeObject:task];
        [_runningTasks addObject:task];
    }
    dispatch_semaphore_signal(_lock);
}

- (void)resumeTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    NSMutableArray *changes = [NSMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (task && [_suspendTasks containsObject:task]) {
            [changes addObject:task];
        }
    }
    [_suspendTasks removeObjectsInArray:changes];
    [_runningTasks addObjectsFromArray:changes];
    dispatch_semaphore_signal(_lock);
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
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        if (cancel && task.state < VVSchedulerTaskStateCanceling) {
            //NSLog(@"---> cancel task: %@", taskId);
            [(NSObject *)task performSelector:@selector(cancel) onThread:self.thread withObject:nil waitUntilDone:NO];
        }
        [_runningTasks removeObject:task];
        [_suspendTasks removeObject:task];
        [_allTasks removeObjectForKey:taskId];
        [_allExtras removeObjectForKey:taskId];
        //NSLog(@"---> remove task: %@", taskId);
    }
    dispatch_semaphore_signal(_lock);
}

- (void)cancelAndRemoveTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    NSMutableArray *changes = [NSMutableArray array];
    for (NSString *taskId in taskIds) {
        VVSchedulerItem task = [_allTasks objectForKey:taskId];
        if (!task) continue;
        if (task.state < VVSchedulerTaskStateCanceling) {
            [(NSObject *)task performSelector:@selector(cancel) onThread:self.thread withObject:nil waitUntilDone:NO];
        }
        [changes addObject:task];
    }
    [_runningTasks removeObjectsInArray:changes];
    [_suspendTasks removeObjectsInArray:changes];
    [_allTasks removeObjectsForKeys:taskIds];
    [_allExtras removeObjectsForKeys:taskIds];
    dispatch_semaphore_signal(_lock);
}

- (void)cancelAndRemoveAllTasks
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    for (VVSchedulerItem task in _allTasks.allValues) {
        if (task.state < VVSchedulerTaskStateCanceling) {
            [(NSObject *)task performSelector:@selector(cancel) onThread:self.thread withObject:nil waitUntilDone:NO];
        }
    }
    [_runningTasks removeAllObjects];
    [_suspendTasks removeAllObjects];
    [_allTasks removeAllObjects];
    [_allExtras removeAllObjects];
    dispatch_semaphore_signal(_lock);
}

- (void)prioritizeTask:(VVSchedulerKey)taskId {
    if (!taskId) return;

    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem task = [_allTasks objectForKey:taskId];
    if (task) {
        //NSLog(@"---> prioritize task: %@", taskId);
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
    dispatch_semaphore_signal(_lock);
}

- (void)prioritizeTasks:(NSArray<VVSchedulerKey> *)taskIds
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
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
    dispatch_semaphore_signal(_lock);
}

- (nullable id<VVSchedulerTask>)objectForKeyedSubscript:(VVSchedulerKey)key
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    VVSchedulerItem task = [_allTasks objectForKey:key];
    dispatch_semaphore_signal(_lock);
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
@property (nonatomic, strong) NSMutableArray *objects;
@property (nonatomic, strong) NSMutableDictionary *keyObjects;
@property (nonatomic, strong) dispatch_semaphore_t lock;
@property (nonatomic, strong) dispatch_queue_t queue;
@end

@implementation VVMergeValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _interval = 0.1;
        _maxMergeCount = 100;
        _objects = [NSMutableArray array];
        _keyObjects = [NSMutableDictionary dictionary];
        _lock = dispatch_semaphore_create(1);
        _queue = dispatch_create_scheduler_queue("merge", DISPATCH_QUEUE_SERIAL);
    }
    return self;
}

- (void)addObject:(id)object forKey:(id<NSCopying>)key
{
    if (!key) return;

    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
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
    dispatch_semaphore_signal(_lock);
    if (self.objects.count >= self.maxMergeCount) {
        [self runMergeOpreation];
    }
}

- (void)runMergeOpreation
{
    if (!self.mergeAction) return;
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    NSArray *objects = self.objects.copy;
    NSDictionary *keyObjects = self.keyObjects.copy;
    [self.objects removeAllObjects];
    [self.keyObjects removeAllObjects];
    dispatch_semaphore_signal(_lock);
    self.mergeAction(objects, keyObjects);
}

@end

@interface VVLimitValve ()
@property (nonatomic, strong) dispatch_queue_t queue;
@property (nonatomic, strong) dispatch_source_t timer;
@property (nonatomic, strong) dispatch_semaphore_t lock;
@property (nonatomic, strong) NSMutableArray *objects;
@property (nonatomic, assign) BOOL running;
@end

@implementation VVLimitValve

- (instancetype)init
{
    self = [super init];
    if (self) {
        _objects = [NSMutableArray array];
        _queue = dispatch_create_scheduler_queue("limit", DISPATCH_QUEUE_SERIAL);
        _lock = dispatch_semaphore_create(1);
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
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    if (self.objects.count == 0 && !_running) {
        _running = YES;
        dispatch_resume(self.timer);
    }
    [self.objects addObject:object];
    dispatch_semaphore_signal(_lock);
}

- (void)poll
{
    dispatch_semaphore_wait(_lock, DISPATCH_TIME_FOREVER);
    if (self.objects.count > 0) {
        id object = self.objects.firstObject;
        [self.objects removeObjectAtIndex:0];
        if (self.objects.count == 0 && _running) {
            dispatch_suspend(self.timer);
            _running = NO;
        }
        if (self.takeAction) self.takeAction(object);
    }
    dispatch_semaphore_signal(_lock);
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
