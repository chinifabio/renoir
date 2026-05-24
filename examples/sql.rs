use indexmap::IndexMap;
use std::io::Write;
use renoir::sql::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tmp_users_table = tempfile::NamedTempFile::new()?;
    writeln!(tmp_users_table, r#"id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35"#)?;


    let input_tables = IndexMap::from([(
        "users".to_string(),
        (
            tmp_users_table.path().to_str().unwrap().to_string(),
            "int,str,int".to_string(),
        ),
    )]);
    
    let output = renoir_sql("SELECT * FROM users WHERE age > 25", "pipelines", None, &input_tables)?;

    println!("Output: {}", output);

    Ok(())
}