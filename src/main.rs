use fs::canonicalize;
use std::path::PathBuf;
use polars::frame::DataFrame;
use polars::prelude::*;
// use polars_lazy::dsl::{all, col, cols, is_not_null};
use polars_lazy::frame::IntoLazy;
use polars_lazy::prelude::concat;
// use polars_lazy::prelude::count;
// use polars_lazy::prelude::*;
// use polars_io::*;
// use polars_io::*;
use std::fs;
// use polars_lazy::prelude::*;
use polars_sql::*;

// const DELTA_PATH: &'static str = "./delta-0.8.0-partitioned";
// const DELTA_PATH: &'static str = "./delta-2.2.0-partitioned-types";
const DELTA_TABLE_PATH: &'static str = "./delta-0.8.0-date";
// const DELTA_TABLE_PATH: &'static str = "./delta-0.8.0-partitioned";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::DeltaTableError> {
    let delta_table = deltalake::open_table(DELTA_TABLE_PATH).await?;
    println!("[Delta table]");
    println!("{delta_table}");
    println!("[schema]{:?}", delta_table.schema());
    println!("[field]{:?}", delta_table.schema().unwrap().get_fields());

    let files = delta_table.get_files();
    let mut df_collection: Vec<DataFrame> = vec![];
    for file_path in files.into_iter() {
        let absolute_path = PathBuf::from(DELTA_TABLE_PATH);


        let full_path = format!("{}/{}", canonicalize(&absolute_path).unwrap().into_os_string().into_string().unwrap(), file_path.as_ref());
        println!("{:?}", full_path);

        let mut file = std::fs::File::open(full_path).unwrap();

        let df = ParquetReader::new(&mut file).finish().unwrap();
        df_collection.push(df);
    }
    let empty_head = df_collection[0].clone().lazy().limit(0);

    let lazy_df = df_collection.into_iter().fold(empty_head, |acc, df| concat([acc, df.lazy()], false, false).unwrap());

    // println!("[COUNT] {:?}", lazy_df.select([count()]).collect().unwrap());

    // println!("[Schema] {:?}", lazy_df.schema());
    // println!("[SELECT ALL] {:?}", lazy_df.select([
    //     cols(["value"])
    // ]).collect());

    // println!("[SELECT ALL] {:?}", lazy_df.select([all()]).collect());
    let mut ctx = SQLContext::new();
    ctx.register("data_fusion_table", lazy_df);

    let query = format!(
        r#"
      SELECT
          date,dayOfYear, sum(dayOfYear) as days
      FROM
          data_fusion_table
      GROUP BY
          date
      "#
    );

    // let query = format!(
    //     r#"
    //   SELECT
    //       *
    //   FROM
    //       data_fusion_table
    //   "#
    // );

    println!("[QUERY OUTCOME]");
    println!("{:?}", ctx.execute(&query).unwrap().collect().unwrap());


    Ok(())
}