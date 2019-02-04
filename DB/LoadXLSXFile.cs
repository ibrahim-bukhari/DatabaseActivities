using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.Data;
using ClosedXML.Excel;
using System.Security;
using System.ComponentModel;

namespace DB
{

    public sealed class LoadXLSXFile : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> InputFile { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> SheetName { get; set; }

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
        
        [Category("Output")]
        public OutArgument<Exception> Exception { get; set; }

        [Category("Output")]
        public OutArgument<DataTable> Table { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string fpath = context.GetValue(this.InputFile);
                string sname = context.GetValue(this.SheetName);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);
                string dbtable = context.GetValue(this.DBTable);
                
                DataTable dt = ImportExceltoDatatable(fpath, sname);
                Console.WriteLine("Number of rows to write: " + dt.Rows.Count);

                Helper.WriteToDB(dbserver, username, password, dt, dbtable);
                Table.Set(context, dt);
                Console.WriteLine("Completed load to DB!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Exception.Set(context, ex);
                throw ex;
            }
        }

        public static DataTable ImportExceltoDatatable(string file_path, string sheet_name)
        {
            Console.Write("Create new datatable");
            //Create a new DataTable.
            DataTable dt = new DataTable();

            // Open the Excel file using ClosedXML.
            // Keep in mind the Excel file cannot be open when trying to read it
            Console.WriteLine("Open workbook: " + file_path);
            using (XLWorkbook workBook = new XLWorkbook(file_path))
            {
                Console.WriteLine("Read sheet: " + sheet_name);
                //Read the Sheet from Excel file.
                IXLWorksheet workSheet = workBook.Worksheet(sheet_name);

                /*
                int rowCounter = 1;

                if (!headers_exist)
                {
                    Console.WriteLine("File does not contain headers. Set generic hearder names for datatable");
                    int cellCount = workSheet.Columns().Count();
                    for (int i = 0; i < cellCount; i++)
                    {
                        string header = String.Format("Col{0}", i);
                        dt.Columns.Add(header);
                    }

                }
                else
                {
                    Console.WriteLine("File contains headers. Set header names from file for datatable");
                    IXLRow row = workSheet.Row(0);
                    foreach (IXLCell cell in row.Cells())
                    {
                        dt.Columns.Add(cell.Value.ToString());
                    }
                    rowCounter++;
                }


                Console.WriteLine("Loop over all rows to fill datatable");
                int lastRow = workSheet.LastRowUsed().RowNumber();
                Console.WriteLine("Last row used: " + lastRow);
                for(int i = rowCounter; i <= lastRow; i++)
                {
                    IXLRow xlRow = workSheet.Row(i);
                    DataRow row = dt.NewRow();
                    for(int j = 0; j < dt.Columns.Count; j++)
                    {
                        DataColumn col = dt.Columns[j];
                        IXLCell c = xlRow.Cell(j);
                        Console.WriteLine("Set value for column: " + col.ColumnName);
                        Console.WriteLine("value is: " + c.Value.ToString());
                        row.SetField(col.ColumnName, c.Value.ToString());
                    }
                    dt.Rows.Add(row);
                    

                }

            */
                
                //Loop through the Worksheet rows.
                bool firstRow = true;
                foreach (IXLRow row in workSheet.Rows())
                {   
                    
                    //Use the first row to add columns to DataTable.
                    if (firstRow)
                    {
                        Console.WriteLine("Read column names from first row");
                        foreach (IXLCell cell in row.Cells())
                        {
                            dt.Columns.Add(cell.Value.ToString());
                        }
                        firstRow = false;
                    }
                    else
                    {
                        //Add rows to DataTable.
                        if (row.FirstCellUsed() != null)
                        {
                            dt.Rows.Add();
                            int i = 0;
                            foreach (IXLCell cell in row.Cells(row.FirstCellUsed().Address.ColumnNumber, row.LastCellUsed().Address.ColumnNumber))
                            {
                                //Console.WriteLine("Write cell value to datatable: " + cell.Value.ToString());
                                if (string.IsNullOrEmpty(cell.Value.ToString()))
                                {
                                    dt.Rows[dt.Rows.Count - 1][i] = DBNull.Value;
                                }
                                else
                                {
                                    Type t = cell.Value.GetType();
                                    switch(t.FullName)
                                    {
                                        case "System.Double":
                                            double d = (double)cell.Value;
                                            dt.Rows[dt.Rows.Count - 1][i] = d.ToString("0." + new string('#', 339));
                                            break;
                                        case "System.String":
                                            dt.Rows[dt.Rows.Count - 1][i] = cell.Value.ToString();
                                            break;
                                        default:
                                            dt.Rows[dt.Rows.Count - 1][i] = cell.Value.ToString();
                                            break;

                                    } 
                                }
                                
                                i++;
                            }
                        }
                    }
                }
                

                return dt;
            }
        }
    }
}
