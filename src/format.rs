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
            // Check if the number is a float and has no fractional part, then cast it to an integer
            if let Some(float_val) = num.as_f64() {
                if float_val.fract() == 0.0 {
                    // Safe to convert to integer
                    Value::Number(serde_json::Number::from(float_val as i64))
                } else {
                    // Keep as float if there is a fractional part
                    value.clone()
                }
            } else {
                value.clone() // If it's not a float, keep it as is
            }
        }
        _ => value.clone(), // For other types, return the value unchanged
    }
}