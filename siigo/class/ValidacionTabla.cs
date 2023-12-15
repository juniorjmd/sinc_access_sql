using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public class ValidacionTabla : IValidacionTabla
    { 
        public ValidacionTabla(string nameTabla)
        {
            Name = nameTabla;
            NameDestino = "";
            ExistInDestinity = false;
            _queryCreateTable = "";
            _queryCreateTableTemp = "";
            _queryDropTableTemp = "";
            _queryClearTable = "";
            _queryLockTable = "";
        }
        public string Name { get; set; }
        public string NameDestino { get; set; }
        public string _queryClearTable { get; set; }
        public bool ExistInDestinity{ get; set; }
        public string _queryCreateTable { get; set; }
        public string _queryLockTable { get; set; }
        public string _queryCreateTableTemp{ get; set; }
        public string _queryDropTableTemp { get; set; } 
        public List<string> _queryInsertDatosDestino{ get; set; }


    }
}
