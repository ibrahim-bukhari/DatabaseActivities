using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.ComponentModel;
using System.Security;
using System.Data;

namespace DB
{

    public sealed class CopyRowWithCondition : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> SourceTable { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> DestinationTable { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Condition { get; set; }
        
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Username { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<SecureString> Password { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Server { get; set; }

        [Category("Output")]
        public OutArgument<Exception> Exception { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);


                string source_table = context.GetValue(this.SourceTable);
                string destination_table = context.GetValue(this.DestinationTable);
                string condition = context.GetValue(this.Condition);

                string col_list_query = @" DECLARE @tableName nvarchar(max) = '{table_name}'
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = @tableName

EXCEPT

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS [tc]
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE [ku] ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
    AND ku.table_name = @tableName
";

                col_list_query = col_list_query.Replace("{table_name}", source_table);
                Console.WriteLine(col_list_query);

                DataTable dt = new DataTable();
                dt = Helper.SelectFromDB(col_list_query, dbserver, username, password);

                StringBuilder sb = new StringBuilder();
                foreach (DataRow dataRow in dt.Rows)
                {
                    foreach (var item in dataRow.ItemArray)
                    {
                        Console.WriteLine(item);
                        sb.Append(string.Format("[{0}]", item));
                        sb.Append(",");
                    }
                }
                string columns_str = sb.ToString().TrimEnd(',');
                Console.WriteLine(columns_str);


                string insert_query = @"INSERT {destination_table} ({destination_col_list})
SELECT {source_col_list}
FROM {source_table}
WHERE {condition};";

                insert_query = insert_query.Replace("{source_table}", source_table);
                insert_query = insert_query.Replace("{source_col_list}", columns_str);
                insert_query = insert_query.Replace("{destination_col_list}", columns_str);
                insert_query = insert_query.Replace("{destination_table}", destination_table);
                insert_query = insert_query.Replace("{condition}", condition);

                Console.WriteLine("------------");
                Console.WriteLine(insert_query);

                Helper.UpdateInDB(insert_query, dbserver, username, password);

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Exception.Set(context, ex);
                throw ex;
            }

        }
    }
}
