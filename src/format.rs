use serde_json::{json, Number, Value};

pub fn format_numbers(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), format_numbers(v)))
                .collect(),
        ),
        Value::Array(arr) => Value::Array(arr.iter().map(format_numbers).collect()),
        Value::Number(num) => num
            .as_f64()
            .and_then(|num| {
                if num.fract() == 0.0 {
                    Some(num as i64)
                } else {
                    None
                }
            })
            .map(Number::from)
            .map(Value::Number)
            .unwrap_or_else(|| value.clone()),
        _ => value.clone(),
    }
}

pub fn format_secrets(data: &Value) -> Value {
    data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"))
        .cloned()
        .unwrap_or_else(|| json!(-1))
}
