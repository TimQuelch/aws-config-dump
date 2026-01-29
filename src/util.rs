pub fn resource_table_name(resource_type: impl AsRef<str>) -> String {
    resource_type
        .as_ref()
        .trim_start_matches("AWS::")
        .replace("::", "_")
        .to_lowercase()
}
