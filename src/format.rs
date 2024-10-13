use serde_json::Value;

pub(crate) fn format_numbers(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (key, val) in map {
                new_map.insert(key.clone(), format_numbers(val));
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => {
            let new_arr: Vec<Value> = arr.iter().map(|val| format_numbers(val)).collect();
            Value::Array(new_arr)
        }
        Value::Number(num) => {
            if let Some(float_val) = num.as_f64() {
                if float_val.fract() == 0.0 {
                    Value::Number(serde_json::Number::from(float_val as i64))
                } else {
                    value.clone()
                }
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    }
}