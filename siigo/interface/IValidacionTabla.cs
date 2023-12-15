using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public interface IValidacionTabla
{
        string Name { get; set; }
        bool ExistInDestinity { get; set; }
        string _queryCreateTable { get; set; }
        string _queryCreateTableTemp { get; set; }
        string _queryDropTableTemp { get; set; }
        List<string> _queryInsertDatosDestino { get; set; }
    }
}
