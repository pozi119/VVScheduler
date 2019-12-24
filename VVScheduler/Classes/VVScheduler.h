//
//  VVScheduler.h
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

typedef NS_ENUM (NSUInteger, VVSchedulerPolicy) {
    VVSchedulerPolicyLIFO,
    VVSchedulerPolicyFIFO,
//    VVSchedulerPolicyPriority,
};

typedef NS_ENUM (NSInteger, VVSchedulerTaskState) {
    VVSchedulerTaskStateRunning   = 0,
    VVSchedulerTaskStateSuspended = 1,
    VVSchedulerTaskStateCanceling = 2,
    VVSchedulerTaskStateCompleted = 3,
};

@protocol VVSchedulerTask <NSObject>
- (id<NSCopying>)identifier;
- (NSInteger)state;
- (void)suspend;
- (void)resume;
- (void)cancel;
- (float)priority;
- (void)setPriority:(float)priority;
- (NSProgress *)progress;
@end

typedef id<VVSchedulerTask>   VVSchedulerItem;
typedef id<NSCopying>         VVSchedulerKey;

@interface VVScheduler : NSObject
@property (nonatomic, assign) NSUInteger maxActivations; ///< default is 8
@property (nonatomic, assign) NSTimeInterval timeout;    ///< if timeout, will suspend temporarily; default is 60s
@property (nonatomic, assign) NSTimeInterval durationOfSuspension; ///< if timeout, duration of suspension; default is 30s
@property (nonatomic, assign, readonly) VVSchedulerPolicy policy;
@property (nonatomic, strong, readonly) NSArray<VVSchedulerItem> *runningTasks;
@property (nonatomic, assign) NSTimeInterval interval;   ///< polling interval, default is 1s, valid only for auto poll
@property (nonatomic, assign) BOOL autoPoll;             ///< default is NO

+ (instancetype)new NS_UNAVAILABLE;
- (instancetype)init NS_UNAVAILABLE;

+ (instancetype)schedulerWithPolicy:(VVSchedulerPolicy)policy;

- (instancetype)initWithPolicy:(VVSchedulerPolicy)policy;

- (void)manualPoll;

- (void)addTask:(VVSchedulerItem)task;

- (void)addTasks:(NSArray<VVSchedulerItem> *)tasks;

- (void)suspendTask:(VVSchedulerKey)taskId;

- (void)suspendTasks:(NSArray<VVSchedulerKey> *)taskIds;

- (void)resumeTask:(VVSchedulerKey)taskId;

- (void)resumeTasks:(NSArray<VVSchedulerKey> *)taskIds;

- (void)removeTask:(VVSchedulerKey)taskId;

- (void)cancelAndRemoveTask:(VVSchedulerKey)taskId;

- (void)cancelAndRemoveTasks:(NSArray<VVSchedulerKey> *)taskIds;

- (void)cancelAndRemoveAllTasks;

- (void)prioritizeTask:(VVSchedulerKey)taskId;

- (void)prioritizeTasks:(NSArray<VVSchedulerKey> *)taskIds;

- (nullable VVSchedulerItem)objectForKeyedSubscript:(VVSchedulerKey)key;

- (void)setObject:(nullable VVSchedulerItem)obj forKeyedSubscript:(VVSchedulerKey)key;

@end

@interface VVMergeValve : NSObject

@property (nonatomic, assign) NSTimeInterval interval; ///< default is 0.1
@property (nonatomic, assign) NSUInteger maxMergeCount; ///< default is 100
@property (nonatomic, copy) void (^ mergeAction)(NSArray *objects, NSDictionary *keyObjects);

- (void)addObject:(id)object forKey:(id<NSCopying>)key;

@end

@interface VVLimitValve : NSObject
@property (nonatomic, assign) NSTimeInterval interval; ///< default is 0.1
@property (nonatomic, copy) void (^ takeAction)(id object);
- (void)addObject:(id)object;

@end

@interface VVLastValve : NSObject
+ (void)setInterval:(NSTimeInterval)interval forIdenitifer:(id<NSCopying>)identifier;
+ (void)doAction:(nonnull void (^)(void))action forIdenitifer:(id<NSCopying>)identifier;
@end

NS_ASSUME_NONNULL_END
