using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Esri.FileGDB;
using GeoAPI.Geometries;
using NetTopologySuite.IO;

namespace Querying
{
    class Querying
    {
        static void Main(string[] args)
        {
            Geodatabase geo = Geodatabase.Open("");

            List<Table> tables = new List<Table>();

            List<string> featureClasses = geo.GetChildDatasets("\\", "Feature Class").ToList();

            foreach (string featureClassName in featureClasses)
            {
                tables.Add(geo.OpenTable(featureClassName));
            }

            string createTableSqlCommand = CreateTableColumns(tables[0].FieldInformation);

            FieldInfo fieldInfo = tables[0].FieldInformation;
            var fields = new List<string>();

            for (int i = 0; i < fieldInfo.Count; i++)
            {
                fields.Add(fieldInfo.GetFieldName(i));  
            }

            var countStatements = $"SELECT * FROM VAULTSCOT_AUTH_POINT_OCT18";
            RowCollection rows = geo.ExecuteSQL(countStatements);
            WKBReader reader = new WKBReader();

            foreach (Row row in rows)
            {
                MultiPointShapeBuffer geometry = row.GetGeometry();
                var point = geometry.Points; 
                //MULTIPOINT (295512,712727,0) throwing format exception - Resolved in upcoming commit.
                var geometryWKT = processMultiPointBuffer(geometry);
            }
            
            var countEnumerator = rows.GetEnumerator();
            List<object> fieldValues = new List<object>();

            if (rows != null)
            {
                countEnumerator.MoveNext();
                var shape = countEnumerator.Current["Shape"];

                for (int i = 0; i < fieldInfo.Count-1; i++)
                {
                    var count = countEnumerator.Current[fields[i]];
                   // Console.WriteLine("field value: " + count);
                    fieldValues.Add(count);
                }
            }

            InsertTableValues(fieldValues, fieldInfo);
        }

        private static string processMultiPointBuffer(MultiPointShapeBuffer buffer)
        {
            try
            {
                string retval = "MULTIPOINT ({0})";
                bool hasZ = false;
                try
                {
                    hasZ = (buffer.Zs != null);
                }
                catch
                {
                    hasZ = false;
                }
                Point[] points = buffer.Points;
                List<string> coords = new List<string>();
                for (int i = 0; i < points.Length; i++)
                {
                    string coord = hasZ ? getCoordinate(points[i].x, points[i].y, buffer.Zs[i]) : getCoordinate(points[i].x, points[i].y);
                    coords.Add(coord);
                }
                string[] coordArray = coords.ToArray();
                string coordList = string.Join(",", coordArray);
                retval = string.Format(retval, coordList);
                return retval;
            }
            catch (Exception ex)
            {
                throw new Exception("Error processing multipoint buffer", ex);
            }
        }

        private static string getCoordinate(double x, double y)
        {
            try
            {
                string retval = x + "," + y;
                return retval;
            }
            catch (Exception ex)
            {
                throw new Exception("Error generating coordinate", ex);
            }
        }

        private static string getCoordinate(double x, double y, double z)
        {
            try
            {
                string retval = x + "," + y + "," + z;
                return retval;
            }
            catch (Exception ex)
            {
                throw new Exception("Error generating coordinate", ex);
            }
        }

        public static string CreateTableColumns(FieldInfo fieldInformation)
        {
            List<string> fieldNames = new List<string>();
            List<FieldType> fieldTypes = new List<FieldType>();

            for (int i = 0; i < fieldInformation.Count; i++)
            {
                fieldNames.Add(fieldInformation.GetFieldName(i));
                fieldTypes.Add(fieldInformation.GetFieldType(i));
            }

            string columnInformation = "";
            for (int i = 0; i < fieldInformation.Count; i++)
            {
                columnInformation += fieldNames[i] + " " + SetSqlDataType(fieldInformation.GetFieldLength(i), fieldInformation.GetFieldType(i)) + ", ";
            }
            columnInformation = columnInformation.TrimEnd(',', ' ');

            string createTableString = "CREATE TABLE GeoDatabaseToSQLTable ( " + columnInformation + "); ";

            return createTableString;
        }

        public static string InsertTableValues(List<object> fieldValues, FieldInfo rowInformation)
        {
            List<string> rowSqlQueries = new List<string>();

            string valueString = "";
            foreach (var item in fieldValues)
            {
                if (item is string e)
                {
                    valueString += "'" + item + "', ";
                }
                else
                {
                    valueString += item + ", ";
                }
            }
            valueString += "null,";

            string insertRowDataQuery = "INSERT INTO GeoDatabaseToSQLTable VALUES ( " + valueString.TrimEnd(',', ' ') + ");"; 


            return insertRowDataQuery;
        }

        public static string SetSqlDataType(int length, FieldType fieldType)
        {
            if (fieldType == FieldType.Integer)
            {
                return "int";
            }

            if (fieldType == FieldType.OID)
            {
                return "int IDENTITY (1,1) PRIMARY KEY";
            }

            if (fieldType == FieldType.String)
            {
                return "nvarchar(" + length + ")";
            }

            if (fieldType == FieldType.Double | fieldType == FieldType.Single)
            {
                return "numeric(38, 8)";
            }

            if (fieldType == FieldType.Blob)
            {
                return "bit";
            }

            if (fieldType == FieldType.Date)
            {
                return "Date";
            }

            if (fieldType == FieldType.Geometry)
            {
                return "geography";
            }

            throw new NotSupportedException(string.Format("The specified column name does not have a corresponding database type."));
        }

        public static IEnumerable<string> EnumerateTables(Geodatabase geo, string path = "\\")
        {
            var e = geo;
            foreach (var table in e.GetChildDatasets(path, "Table"))
                yield return "Table: " + table;

            foreach (var featureClass in e.GetChildDatasets(path, "Feature Class"))
                yield return "Feature Class: " + featureClass;

            foreach (var featureDataset in e.GetChildDatasets(path, "Feature Dataset"))
            {
                yield return "Feature Dataset: " + featureDataset;
                foreach (var featureData in EnumerateTables(geo, featureDataset))
                {
                    yield return featureData;
                }
            }
        }
    }
}
