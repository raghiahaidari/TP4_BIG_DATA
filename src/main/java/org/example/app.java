package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class app {

    public static void main(String[] args) throws Exception {
        // Configuration de HBase
        Configuration config = HBaseConfiguration.create();

        // Connexion à HBase
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();

            // Création de la table Students avec les familles de colonnes info et grades
            TableName tableName = TableName.valueOf("Students");
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("grades"))
                    .build();
            admin.createTable(tableDescriptor);

            // Ajout des étudiants
            addStudent(connection, "student1", "John Doe", "20", "B", "A");
            addStudent(connection, "student2", "Jane Smith", "22", "A", "A");

            // Récupération et affichage des informations pour "student1"
            getStudentInfo(connection, "student1");

            // Mise à jour de l'âge de "Jane Smith" à "23" et sa note de math à "A+"
            updateStudent(connection, "student2", "Jane Smith", "23", "A+", "A");

            // Suppression de l'étudiant avec la Row Key "student1"
            deleteStudent(connection, "student1");

            // Affichage de toutes les informations pour tous les étudiants
            scanStudents(connection);
        }
    }

    private static void addStudent(Connection connection, String rowKey, String name, String age, String mathGrade, String scienceGrade) throws Exception {
        Table table = connection.getTable(TableName.valueOf("Students"));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
        put.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("math"), Bytes.toBytes(mathGrade));
        put.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("science"), Bytes.toBytes(scienceGrade));
        table.put(put);
        table.close();
    }

    private static void getStudentInfo(Connection connection, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf("Students"));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("name")).forEach(cell -> {
            System.out.println("Name: " + Bytes.toString(CellUtil.cloneValue(cell)));
        });
        result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age")).forEach(cell -> {
            System.out.println("Age: " + Bytes.toString(CellUtil.cloneValue(cell)));
        });
        result.getColumnCells(Bytes.toBytes("grades"), Bytes.toBytes("math")).forEach(cell -> {
            System.out.println("Math Grade: " + Bytes.toString(CellUtil.cloneValue(cell)));
        });
        result.getColumnCells(Bytes.toBytes("grades"), Bytes.toBytes("science")).forEach(cell -> {
            System.out.println("Science Grade: " + Bytes.toString(CellUtil.cloneValue(cell)));
        });
        table.close();
    }

    private static void updateStudent(Connection connection, String rowKey, String name, String age, String mathGrade, String scienceGrade) throws Exception {
        Table table = connection.getTable(TableName.valueOf("Students"));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
        put.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("math"), Bytes.toBytes(mathGrade));
        put.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("science"), Bytes.toBytes(scienceGrade));
        table.put(put);
        table.close();
    }

    private static void deleteStudent(Connection connection, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf("Students"));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    private static void scanStudents(Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf("Students"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("name")).forEach(cell -> {
                System.out.println("Name: " + Bytes.toString(CellUtil.cloneValue(cell)));
            });
            result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age")).forEach(cell -> {
                System.out.println("Age: " + Bytes.toString(CellUtil.cloneValue(cell)));
            });
            result.getColumnCells(Bytes.toBytes("grades"), Bytes.toBytes("math")).forEach(cell -> {
                System.out.println("Math Grade: " + Bytes.toString(CellUtil.cloneValue(cell)));
            });
            result.getColumnCells(Bytes.toBytes("grades"), Bytes.toBytes("science")).forEach(cell -> {
                System.out.println("Science Grade: " + Bytes.toString(CellUtil.cloneValue(cell)));
            });
        }
        table.close();
    }
}
