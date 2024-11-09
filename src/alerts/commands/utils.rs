use metricsql_parser::parser::is_valid_identifier;
use valkey_module::ValkeyError;
use valkey_module::ValkeyResult;

pub(super) fn validate_group_name(name: &str) -> ValkeyResult<()> {

    if name.is_empty() {
        return Err(ValkeyError::Str("ERR missing group name"));
    }
    
    if !is_valid_identifier(name) {
        return Err(ValkeyError::Str("ERR invalid group name"));
    }

    Ok(())
}
