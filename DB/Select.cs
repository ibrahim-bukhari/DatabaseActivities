using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.Data;
using System.ComponentModel;
using System.Security;

namespace DB
{

    public sealed class Select : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<String> Query { get; set; }
        
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

        [Category("Output")]
        [RequiredArgument]
        public OutArgument<DataTable> Table { get; set; }

        // If your activity returns a value, derive from CodeActivity<TResult>
        // and return the value from the Execute method.
        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string query = context.GetValue(this.Query);
                string username = context.GetValue(this.Username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);

                DataTable dt = Helper.SelectFromDB(query, dbserver, username, password);
                Console.WriteLine("Number of rows in table: " + dt.Rows.Count);
                Table.Set(context, dt);
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
