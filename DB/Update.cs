using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Activities;
using System.ComponentModel;
using System.Security;

namespace DB
{

    public sealed class Update : CodeActivity
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


        // If your activity returns a value, derive from CodeActivity<TResult>
        // and return the value from the Execute method.
        protected override void Execute(CodeActivityContext context)
        {
            try
            {
                string query = context.GetValue(this.Query);
                Console.WriteLine("Query: " + query);
                string username = context.GetValue(this.Username);
                Console.WriteLine("Username: " + username);
                SecureString password = context.GetValue(this.Password);
                string dbserver = context.GetValue(this.Server);
                Console.WriteLine("Server: " + dbserver);

                Helper.UpdateInDB(query, dbserver, username, password);
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
