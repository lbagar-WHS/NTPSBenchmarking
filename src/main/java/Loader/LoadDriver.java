package Loader;

import java.sql.*;

public class LoadDriver
{
    public void startBenchmarking()
    {
        try
        {
            //JDBC Treiber im Classpath hinzufügen
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            //Connection zur DB aufbauen
            Connection con = DriverManager.getConnection(
                    "jdbc:sqlserver://LAP-BOCHOLT233\\SQLEXPRESS:1433;database=DBI;encrypt=true;trustServerCertificate=true;useBulkCopyForBatchInsert=true;",
                    "dbi",
                    "dbi_pass"
            );

            con.setAutoCommit(false);

            System.out.println("Starting Benchmarking...");

            long starttime              = System.currentTimeMillis();
            int countTransaktions       = 0;
            boolean isEinschwingphase   = true;
            boolean isMessphase         = false;
            boolean isAusschwingphase   = false;

            while ((System.currentTimeMillis() - starttime) <= 600000)
            {
                if(isMessphase && (System.currentTimeMillis() - starttime) >= 540000)
                {
                    isMessphase         = false;
                    isAusschwingphase   = true;

                    System.out.println("Messphase wurde beendet, Ausschwingphase gestartet...");
                    System.out.println("Anzahl Transaktionen insgesamt: " + countTransaktions);
                    System.out.println("Anzahl Transaktionen pro Sekunde Durchschnitt: " + countTransaktions /5 /60);
                }
                else if(isEinschwingphase && (System.currentTimeMillis() - starttime) >= 240000)
                {
                    isMessphase         = true;
                    isEinschwingphase   = false;

                    System.out.println("Messphase wird gestartet...");
                }

                executeRandomFunction(con);

                if(isMessphase)
                {
                    countTransaktions++;
                }

                Thread.sleep(50);
            }

            con.close();

            System.out.println("Finished Benchmarking...");
        }
        catch(InterruptedException | ClassNotFoundException | SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void executeRandomFunction(Connection con) throws SQLException
    {
        double random = Math.random();

        if(random < 0.15)
        {
            getAnzahlEinzahlung(con, getRandomInt(10000));
        }
        else if(random < 0.35)
        {
            getKontostand(con, getRandomInt(10000000));
        }
        else
        {
            insertEinzahlung(con, getRandomInt(10000000), getRandomInt(1000), getRandomInt(100), getRandomInt(10000));
        }
    }

    public void testExecuteFunction()
    {
        try
        {
            //JDBC Treiber im Classpath hinzufügen
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            //Connection zur DB aufbauen
            Connection con = DriverManager.getConnection(
                    "jdbc:sqlserver://LAP-BOCHOLT233\\SQLEXPRESS:1433;database=DBI;encrypt=true;trustServerCertificate=true;useBulkCopyForBatchInsert=true;",
                    "dbi",
                    "dbi_pass"
            );

            con.setAutoCommit(false);

            System.out.println("Kontostand Account 5: " + getKontostand(con, 5));

            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(con, 5, 1, 1, 500));
            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(con, 5, 1, 1, 500));
            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(con, 5, 1, 1, 500));

            System.out.println("Anzahl Einzahlung 500€: " + getAnzahlEinzahlung(con, 500));

            System.out.println("Kontostand Account 5: " + getKontostand(con, 5));

            con.close();
        }
        catch (ClassNotFoundException | SQLException e)
        {
            throw new RuntimeException(e);
        }


    }

    private int getRandomInt(int max)
    {
        int range = max - 2;

        return (int) (Math.random() * range) + 1;
    }

    private int getKontostand(Connection con, int ACCID) throws SQLException
    {
        Statement statement = con.createStatement();

        boolean execute = statement.execute("SELECT balance FROM accounts WHERE ACCID = " + ACCID);
        
        if(execute)
        {
            ResultSet resultSet = statement.getResultSet();

            if(resultSet.next())
            {
                return resultSet.getInt(1);
            }
        }

        con.commit();

        return -1;
    }

    private int insertEinzahlung(Connection con, int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException
    {
        Statement statement = con.createStatement();

        statement.execute("UPDATE BRANCHES SET balance = balance + " + DELTA + " WHERE BRANCHID = " + BRANCHID);
        statement.execute("UPDATE TELLERS  SET balance = balance + " + DELTA + " WHERE TELLERID = " + TELLERID);
        statement.execute("UPDATE ACCOUNTS SET balance = balance + " + DELTA + " WHERE ACCID    = " + ACCID);

        int kontostand = getKontostand(con, ACCID);

        statement.execute("INSERT INTO HISTORY (ACCID, TELLERID, DELTA, BRANCHID, ACCBALANCE, CMMNT) " +
                                "VALUES (" + ACCID + ", " + TELLERID + ", " + DELTA + ", " + BRANCHID + ", " + kontostand + ", 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')");

        con.commit();

        return kontostand;
    }

    private int getAnzahlEinzahlung(Connection con, int DELTA) throws SQLException
    {
        Statement statement = con.createStatement();

        boolean execute = statement.execute("SELECT COUNT(*) FROM history WHERE delta = " + DELTA);

        if(execute)
        {
            ResultSet resultSet = statement.getResultSet();

            if(resultSet.next())
            {
                return resultSet.getInt(1);
            }
        }

        con.commit();

        return 0;
    }
}
