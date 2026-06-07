use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    CountDistinct,
    Sum,
    Min,
    Max,
    Avg,
    Collect,
    GroupConcat,
    Median,
    CollectDistinct,
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
}
