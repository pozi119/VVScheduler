//
//  VVThreadMutableArray.m
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import "VVThreadMutableArray.h"

@interface VVThreadMutableArray ()
@property (nonatomic, strong) NSRecursiveLock *lock;
@end

@implementation VVThreadMutableArray
{
    CFMutableArrayRef _array;
}

- (instancetype)init
{
    self = [super init];
    if (self) {
        _array = CFArrayCreateMutable(kCFAllocatorDefault, 10,  &kCFTypeArrayCallBacks);
        _lock = [[NSRecursiveLock alloc] init];
    }
    return self;
}

- (instancetype)initWithCapacity:(NSUInteger)numItems
{
    self = [super init];
    if (self) {
        _array = CFArrayCreateMutable(kCFAllocatorDefault, numItems,  &kCFTypeArrayCallBacks);
        _lock = [[NSRecursiveLock alloc] init];
    }
    return self;
}

//MARK: - primitive methods

- (NSUInteger)count
{
    NSUInteger result = 0;
    [_lock lock];
    result = CFArrayGetCount(self->_array);
    [_lock unlock];
    return result;
}

- (id)objectAtIndex:(NSUInteger)index
{
    id result = nil;
    [_lock lock];
    NSUInteger count = CFArrayGetCount(self->_array);
    result = index < count ? CFArrayGetValueAtIndex(self->_array, index) : nil;
    [_lock unlock];
    return result;
}

- (void)addObject:(id)anObject
{
    [_lock lock];
    if (anObject) {
        CFArrayAppendValue(self->_array, (__bridge const void *)anObject);
    }
    [_lock unlock];
}

- (void)addObjectsFromArray:(NSArray<id> *)otherArray
{
    [_lock lock];
    for (id temp in otherArray) {
        CFArrayAppendValue(self->_array, (__bridge const void *)temp);
    }
    [_lock unlock];
}

- (void)insertObject:(id)anObject atIndex:(NSUInteger)index
{
    [_lock lock];
    if (anObject) {
        NSUInteger count = CFArrayGetCount(self->_array);
        NSUInteger blockIndex = index > count ? count : index;
        CFArrayInsertValueAtIndex(self->_array, blockIndex, (__bridge const void *)anObject);
    }
    [_lock unlock];
}

- (void)removeLastObject
{
    [_lock lock];
    NSUInteger count = CFArrayGetCount(self->_array);
    if (count > 0) {
        CFArrayRemoveValueAtIndex(self->_array, count - 1);
    }
    [_lock unlock];
}

- (void)removeObjectAtIndex:(NSUInteger)index
{
    [_lock lock];
    NSUInteger count = CFArrayGetCount(self->_array);
    if (index < count) {
        CFArrayRemoveValueAtIndex(self->_array, index);
    }
    [_lock unlock];
}

- (void)replaceObjectAtIndex:(NSUInteger)index withObject:(id)anObject
{
    [_lock lock];
    if (anObject) {
        NSUInteger count = CFArrayGetCount(self->_array);
        if (index >= count) {
            CFArraySetValueAtIndex(self->_array, index, (__bridge const void *)anObject);
        }
    }
    [_lock unlock];
}

- (void)removeAllObjects
{
    [_lock lock];
    CFArrayRemoveAllValues(self->_array);
    [_lock unlock];
}

- (NSUInteger)indexOfObject:(id)anObject
{
    NSUInteger index = NSNotFound;
    [_lock lock];
    NSUInteger count = CFArrayGetCount(self->_array);
    index = CFArrayGetFirstIndexOfValue(self->_array, CFRangeMake(0, count), (__bridge const void *)(anObject));
    [_lock unlock];
    return index;
}

@end
