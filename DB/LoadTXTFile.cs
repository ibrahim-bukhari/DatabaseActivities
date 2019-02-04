using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.Security;
using System.ComponentModel;
using System.Data;
using System.IO;

namespace DB
{

    public sealed class LoadTXTFile : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> InputFile { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Server { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> DBTable { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Username { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<SecureString> Password { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<int> RowsToSkip { get; set; }

        [Category("Output")]
        public OutArgument<Exception> Exception { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string fpath = context.GetValue(this.InputFile);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);
                string dbtable = context.GetValue(this.DBTable);
                int skipCount = context.GetValue(this.RowsToSkip);

                DataTable dt = GetTableFromTXTFile(fpath, skipCount);

                Helper.WriteToDB(dbserver, username, password, dt, dbtable);
                Console.WriteLine("Completed load to DB!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Exception.Set(context, ex);
                throw ex;
            }
        }

        private DataTable GetTableFromTXTFile(string input_path, int skip_count)
        {
            DataTable table = new DataTable();
            string[] cols = AddColumnsToTable(input_path, table);
            AddRowsToTable(input_path, table, skip_count, cols);
            return table;
        }

        private string[] AddColumnsToTable(string input_path, DataTable dt)
        {
            //Use the first row to add columns to DataTable.
            string[] cols;
            var fileStream = new FileStream(@input_path, FileMode.Open, FileAccess.Read);
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                string text = streamReader.ReadLine();
                string line = text.Trim();
                cols = line.Split('\t');
                foreach (string s in cols)
                {
                    if (!string.IsNullOrEmpty(s))
                    {
                        dt.Columns.Add(s);
                    }
                }
            }
            return cols;
        }

        private void AddRowsToTable(string input_path, DataTable dt, int skip_count, string[] col_list)
        {
            //Add rows to DataTable.
            var fileStream = new FileStream(input_path, FileMode.Open, FileAccess.Read);
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                Console.WriteLine("Number of rows to skip: " + skip_count);
                //skip unnecessary rows from top
                for (var i = 0; i < skip_count; i++)
                {
                    streamReader.ReadLine();
                }

                Console.WriteLine("Start reading line by line");
                while (streamReader.Peek() >= 0)
                {
                    string text = streamReader.ReadLine();
                    if (string.IsNullOrEmpty(text))
                    {
                        Console.WriteLine("--- IGNORE EMPTY LINE ---");
                        continue;
                    }
                    string line = text.Trim();
                    DataRow row;
                    row = dt.NewRow();
                    string[] items = line.Split('\t');
                    for (int i = 0; i < items.Length; i++)
                    {
                        if (!string.IsNullOrEmpty(items[i]))
                        {
                            items[i] = items[i].Trim();
                            row.SetField(col_list[i], items[i]);

                        }
                    }
                }
            }
        }
    }
}
