using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.Security;
using System.ComponentModel;
using System.Data;
using ClosedXML.Excel;

namespace DB
{

    public sealed class ExportTableToXLSX : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Query { get; set; }

        [Category("Input")]
        public InArgument<String> SheetName { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> OutputPath { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Server { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Username { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<SecureString> Password { get; set; }

        [Category("Output")]
        public OutArgument<Exception> Exception { get; set; }
        
        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string query = context.GetValue(this.Query);
                string sheet = context.GetValue(this.SheetName);
                string output = context.GetValue(this.OutputPath);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);

                DataTable dt = Helper.SelectFromDB(query, dbserver, username, password);
                Console.WriteLine("Number of rows to write: " + dt.Rows.Count);

                WriteTableToFile(dt, output, sheet);

                Console.WriteLine("Completed XLSX write from DB!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Exception.Set(context, ex);
                throw ex;
            }
            finally
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
            }

        }

        private void WriteTableToFile(DataTable table, string file_path, string sheet_name)
        {
            XLWorkbook wb = new XLWorkbook();
            string worksheetName = (string.IsNullOrEmpty(sheet_name) ? "Sheet1" : sheet_name);
            wb.Worksheets.Add(table, worksheetName);
            wb.SaveAs(file_path);
            wb.Dispose();
        }
    }
}
