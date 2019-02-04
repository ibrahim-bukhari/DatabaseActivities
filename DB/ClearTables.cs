using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.ComponentModel;
using System.Security;

namespace DB
{

    public sealed class ClearTables : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Tables { get; set; }

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


        // If your activity returns a value, derive from CodeActivity<TResult>
        // and return the value from the Execute method.
        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string listOfTables = context.GetValue(this.Tables);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);

                string[] tableNames = listOfTables.Split(',');
                foreach (string table in tableNames)
                {
                    string query = string.Format("delete from {0};", table);
                    Helper.SelectFromDB(query, dbserver, username, password);
                }
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
