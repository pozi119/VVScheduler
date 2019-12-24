//
//  VVThreadMutableDictionary.m
//  VVScheduler
//
//  Created by Valo on 2019/12/24.
//

#import "VVThreadMutableDictionary.h"

@interface VVThreadMutableDictionary ()
@property (nonatomic, strong) NSRecursiveLock *lock;
@end

@implementation VVThreadMutableDictionary
{
    CFMutableDictionaryRef _dictionary;
}

- (instancetype)init
{
    self = [super init];
    if (self) {
        _dictionary = CFDictionaryCreateMutable(kCFAllocatorDefault, 10, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
        _lock = [[NSRecursiveLock alloc] init];
    }
    return self;
}

- (instancetype)initWithCapacity:(NSUInteger)numItems
{
    self = [super init];
    if (self) {
        _dictionary = CFDictionaryCreateMutable(kCFAllocatorDefault, numItems, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
        _lock = [[NSRecursiveLock alloc] init];
    }
    return self;
}

- (instancetype)initWithObjects:(id _Nonnull const [])objects forKeys:(id<NSCopying>  _Nonnull const [])keys count:(NSUInteger)cnt {
    self = [super init];
    if (self) {
        _dictionary = CFDictionaryCreateMutable(kCFAllocatorDefault, cnt, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
        for (NSUInteger i = 0; i < cnt; i++) {
            id value = objects[i];
            id<NSCopying> key = keys[i];
            if (value && key) {
                CFDictionaryAddValue(_dictionary, (__bridge const void *)(key), (__bridge const void *)(value));
            }
        }
        _lock = [[NSRecursiveLock alloc] init];
    }
    return self;
}

//MARK: - primitive methods
- (NSUInteger)count
{
    NSUInteger result = 0;
    [_lock lock];
    result = CFDictionaryGetCount(self->_dictionary);
    [_lock unlock];
    return result;
}

- (id)objectForKey:(id)aKey
{
    __block id result = nil;
    [_lock lock];
    if (aKey) {
        result = CFDictionaryGetValue(self->_dictionary, CFBridgingRetain(aKey));
    }
    [_lock unlock];
    return result;
}

- (void)setObject:(id)anObject forKey:(id<NSCopying>)aKey
{
    [_lock lock];
    if (aKey) {
        if (anObject) {
            CFDictionaryAddValue(self->_dictionary, (__bridge const void *)(aKey), (__bridge const void *)(anObject));
        } else {
            CFDictionaryRemoveValue(self->_dictionary, (__bridge const void *)(aKey));
        }
    }
    [_lock unlock];
}

- (void)removeObjectForKey:(id)aKey
{
    [_lock lock];
    if (aKey) {
        CFDictionaryRemoveValue(self->_dictionary, (__bridge const void *)(aKey));
    }
    [_lock unlock];
}

- (NSEnumerator *)keyEnumerator
{
    NSEnumerator *enumerator = nil;

    [_lock lock];
    CFIndex count;
    const void **keys;

    count = CFDictionaryGetCount(_dictionary);
    keys = (const void **)malloc(sizeof(void *) * count);
    CFDictionaryGetKeysAndValues(_dictionary, keys, NULL);

    CFArrayRef array = CFArrayCreate(kCFAllocatorDefault, keys, count, &kCFTypeArrayCallBacks);
    free((void *)keys);

    enumerator = [(__bridge NSArray *)array objectEnumerator];
    [_lock unlock];

    return enumerator;
}

- (void)addEntriesFromDictionary:(NSDictionary *)otherDictionary
{
    [_lock lock];
    [otherDictionary enumerateKeysAndObjectsUsingBlock:^(id aKey, id anObject, BOOL *stop) {
        CFDictionaryAddValue(self->_dictionary, (__bridge const void *)(aKey), (__bridge const void *)(anObject));
    }];
    [_lock unlock];
}

- (void)removeAllObjects
{
    [_lock lock];
    CFDictionaryRemoveAllValues(self->_dictionary);
    [_lock unlock];
}

@end
