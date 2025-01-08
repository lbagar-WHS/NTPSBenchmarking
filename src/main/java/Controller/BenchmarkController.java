package Controller;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BenchmarkController
{
    public void deleteHistoryEntries()
    {
        //JDBC Treiber im Classpath hinzufügen
        try
        {
            System.out.println("Daten werden aus HISTORY entfernt");

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            //Connection zur DB aufbauen
            Connection con = DriverManager.getConnection(
                    "jdbc:sqlserver://LAP-BOCHOLT233\\SQLEXPRESS:1433;database=DBI;encrypt=true;trustServerCertificate=true",
                    "dbi",
                    "dbi_pass"
            );

            con.setAutoCommit(false);

            Statement stmt = con.createStatement();

            //Falls Werte vorhanden werden, werden diese aus den Tabellen gelöscht
            stmt.execute("DROP TABLE IF EXISTS dbi.history;");
            stmt.execute("""
                        create table dbi.history
                        ( accid int not null,
                        tellerid int not null,
                        delta int not null,
                        branchid int not null,
                        accbalance int not null,
                        cmmnt char(30) not null,
                        foreign key (accid) references accounts,
                        foreign key (tellerid) references tellers,
                        foreign key (branchid) references branches );
                        """);

            con.commit();
            stmt.close();
            con.close();

            System.out.println("Daten aus HISTORY entfernt");
        }
        catch (ClassNotFoundException | SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void initDB(int n)
    {
        //try Block um ClassNotFoundException und SQLException abzufangen
        try
        {
            System.out.println("Start init DB");

            int anzahlRowBatch      = 1000;

            //JDBC Treiber im Classpath hinzufügen
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            //Connection zur DB aufbauen
            Connection con = DriverManager.getConnection(
                    "jdbc:sqlserver://LAP-BOCHOLT233\\SQLEXPRESS:1433;database=DBI;encrypt=true;trustServerCertificate=true",
                    "dbi",
                    "dbi_pass"
            );

            con.setAutoCommit(false);

            Statement stmt = con.createStatement();

            //Falls Werte vorhanden werden, werden diese aus den Tabellen gelöscht
            stmt.execute("DROP TABLE IF EXISTS dbi.history;");
            stmt.execute("DROP TABLE IF EXISTS dbi.accounts;");
            stmt.execute("DROP TABLE IF EXISTS dbi.tellers;");
            stmt.execute("DROP TABLE IF EXISTS dbi.branches;");

            //Falls Tabellen in DB noch nicht existieren, werden diese erstellt
            //Befehl Create Table IF Not Exists gibt es nicht, daher IF Abfrage in SQL
            stmt.execute("""
                        create table dbi.branches
                        (branchid int not null,
                        branchname char(20) not null,
                        balance int not null,
                        address char(72) not null,
                        primary key (branchid) );""");

            stmt.execute("""
                        create table dbi.accounts
                        ( accid int not null,
                        name char(20) not null,
                        balance int not null,
                        branchid int not null,
                        address char(68) not null,
                        primary key (accid),
                        foreign key (branchid) references branches );""");

            stmt.execute("""
                        create table dbi.tellers
                        ( tellerid int not null,
                        tellername char(20) not null,
                        balance int not null,
                        branchid int not null,
                        address char(68) not null,
                        primary key (tellerid),
                        foreign key (branchid) references branches );""");

            stmt.execute("""
                        create table dbi.history
                        ( accid int not null,
                        tellerid int not null,
                        delta int not null,
                        branchid int not null,
                        accbalance int not null,
                        cmmnt char(30) not null,
                        foreign key (accid) references accounts,
                        foreign key (tellerid) references tellers,
                        foreign key (branchid) references branches );
                        """);

            stmt.close();

            System.out.println("Daten aus Datenbanken entfernt");

            System.out.println("Start SQL Statements erstellen");

            long startTimeMili = System.currentTimeMillis();

            System.out.println("Start SQL Statements ausführen");

            //Multiplikatoren von benötigte Tupel
            int branchMultiplier    = 1;
            int accountMultiplier   = 100000;
            int tellerMultiplier    = 10;
            int rowsCounterBatch    = 0;

            //Tupel für branches hinzufügen
            PreparedStatement preparedStatementBranch = con.prepareStatement("INSERT INTO dbi.branches (branchid, branchname, balance, address) VALUES (?, ?, ?, ?)");

            for(int i = 1; i <= n * branchMultiplier; i++)
            {
                rowsCounterBatch++;

                preparedStatementBranch.setInt      (1, i);
                preparedStatementBranch.setString   (2, "AAAAAAAAAAAAAAAAAAAA");
                preparedStatementBranch.setInt      (3, 0);
                preparedStatementBranch.setString   (4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

                preparedStatementBranch.addBatch();

                if(rowsCounterBatch % anzahlRowBatch == 0)
                {
                    rowsCounterBatch = 0;
                    preparedStatementBranch.executeBatch();
                    con.commit();
                }
            }

            if(rowsCounterBatch > 0)
            {
                rowsCounterBatch = 0;
                preparedStatementBranch.executeBatch();
                con.commit();
            }

            preparedStatementBranch.close();

            System.out.println("Branches hinzugefügt");

            //Tupel für accounts hinzufügen
            PreparedStatement preparedStatementAccounts = con.prepareStatement("INSERT INTO dbi.accounts (accid, name, balance, address, branchid) VALUES (?, ?, ?, ?, ?)");

            for(int i = 1; i <= n * accountMultiplier; i++)
            {
                rowsCounterBatch++;

                preparedStatementAccounts.setInt      (1, i);
                preparedStatementAccounts.setString   (2, "AAAAAAAAAAAAAAAAAAAA");
                preparedStatementAccounts.setInt      (3, 0);
                preparedStatementAccounts.setString   (4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                preparedStatementAccounts.setInt      (5, randomBranchID(n));

                preparedStatementAccounts.addBatch();

                if(rowsCounterBatch % anzahlRowBatch == 0)
                {
                    rowsCounterBatch = 0;
                    preparedStatementAccounts.executeBatch();
                    con.commit();
                    //System.out.println("Anzahl Accounts hinzugefügt: " + i);
                }
            }

            if(rowsCounterBatch > 0)
            {
                rowsCounterBatch = 0;
                preparedStatementAccounts.executeBatch();
                con.commit();
            }

            preparedStatementAccounts.close();

            System.out.println("Accounts hinzugefügt");

            //Tupel für tellers hinzufügen
            PreparedStatement preparedStatementTeller = con.prepareStatement("INSERT INTO dbi.tellers (tellerid, tellername, balance, address, branchid) VALUES (?, ?, ?, ?, ?)");

            for(int i = 1; i <= n * tellerMultiplier; i++)
            {
                rowsCounterBatch++;

                preparedStatementTeller.setInt      (1, i);
                preparedStatementTeller.setString   (2, "AAAAAAAAAAAAAAAAAAAA");
                preparedStatementTeller.setInt      (3, 0);
                preparedStatementTeller.setString   (4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                preparedStatementTeller.setInt      (5, randomBranchID(n));

                preparedStatementTeller.addBatch();

                if(rowsCounterBatch % anzahlRowBatch == 0)
                {
                    rowsCounterBatch = 0;
                    preparedStatementTeller.executeBatch();
                    con.commit();
                }
            }

            if(rowsCounterBatch > 0)
            {
                preparedStatementTeller.executeBatch();
                con.commit();
            }

            preparedStatementTeller.close();

            System.out.println("Tellers hinzugefügt");

            long durationMili = System.currentTimeMillis() - startTimeMili;

            System.out.println(durationMili / 1000 + "s wurden zum einfügen gebarucht");

            //Verbindung schließen
            con.close();
        }
        catch (ClassNotFoundException | SQLException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private int randomBranchID(int n)
    {
        //zufällige BranchID auswählen zwischen 1 und n
        return randomInt(1, n);
    }

    private int randomInt(int low, int high)
    {
        //zufallszahl zwischen low und high auswählen
        Random r = new Random();

        if(low == high)
            return low;

        return r.nextInt(high-low) + low;
    }

    private String randomString(int length)
    {
        //zufälliger String mit der Länge length erstellen und ausgeben
        String result = "";

        for (int i = 0; i < length; i++)
        {
            result += (char) randomInt(97, 122);
        }

        return result;
    }
}
