using System;
using System.Activities;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace DB
{
    public class LoadDatatable : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<DataTable> Table { get; set; }

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
        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                DataTable dt = context.GetValue(this.Table);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);
                string dbtable = context.GetValue(this.DBTable);


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
    }
}
