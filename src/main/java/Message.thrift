struct Message {
    1: i32 id
    2: string name
    3: string type
    4: i64 timestamp
    5: i32 correlationId

    6: Flow flow
    7: User user
    8: Entity entity
    9: Operation operation
}

struct User {
    1: string id
    2: string idName
    3: string companyId
    4: string companyIdName
}

struct Flow {
    1: string id
    2: string idName
    3: string name
    4: string variant
    5: string variantName
}

struct Entity {
    1: string id
    2: string idName
    3: string name
}

struct Operation {
    1: string name
    2: string serviceName
    3: string environment
    4: i64 timestamp
    5: i32 durationUs
}