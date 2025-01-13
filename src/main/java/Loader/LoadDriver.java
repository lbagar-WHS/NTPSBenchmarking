package Loader;

import java.sql.*;
import java.util.Random;

public class LoadDriver implements Runnable
{
    private final Random random;
    private String name = "";

    public static int totalAnzahlEinzahlungenTransactions = 0;
    public static int totalKontostandTransactions = 0;
    public static int totalEinzahlungTransactions = 0;

    private boolean isMessphase = false;

    private PreparedStatement getKontostandPrepared;
    private PreparedStatement updateBrancheBalancePrepared;
    private PreparedStatement updateTellerBalancePrepared;
    private PreparedStatement updateAccountBalancePrepared;
    private PreparedStatement insertTransactionIntoHistoryPrepared;

    private PreparedStatement getAnzahlEinzahlungPrepared;

    public LoadDriver(String name, int seed)
    {
        this.name   = name;
        this.random = new Random(seed);
    }

    public void run()
    {
        try
        {
            //Connection zur DB aufbauen
            Connection con = DriverManager.getConnection(
                    "jdbc:sqlserver://LAP-BOCHOLT233\\SQLEXPRESS:1433;database=DBI;encrypt=true;trustServerCertificate=true;useBulkCopyForBatchInsert=true;",
                    "dbi",
                    "dbi_pass"
            );

            con.setAutoCommit(false);

            System.out.println("LoadDriver " + name + ": Starting Benchmarking...");

            getKontostandPrepared                   = con.prepareStatement("SELECT balance FROM ACCOUNTS WHERE ACCID = ?");

            updateBrancheBalancePrepared            = con.prepareStatement("UPDATE BRANCHES SET balance = balance + ? WHERE BRANCHID = ?");
            updateTellerBalancePrepared             = con.prepareStatement("UPDATE TELLERS  SET balance = balance + ? WHERE TELLERID = ?");
            updateAccountBalancePrepared            = con.prepareStatement("UPDATE ACCOUNTS SET balance = balance + ? WHERE ACCID    = ?");
            insertTransactionIntoHistoryPrepared    = con.prepareStatement("INSERT INTO HISTORY (ACCID, TELLERID, DELTA, BRANCHID, ACCBALANCE, CMMNT) VALUES(?, ?, ?, ?, ?, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')");

            getAnzahlEinzahlungPrepared             = con.prepareStatement("SELECT COUNT(*) FROM history WHERE delta = ?");

            long starttime              = System.currentTimeMillis();
            boolean isEinschwingphase   = true;

            while ((System.currentTimeMillis() - starttime) <= 600000)
            {
                if(isMessphase && (System.currentTimeMillis() - starttime) >= 540000)
                {
                    isMessphase         = false;

                    System.out.println("LoadDriver " + name + ": Messphase wurde beendet, Ausschwingphase gestartet...");
                }
                else if(isEinschwingphase && (System.currentTimeMillis() - starttime) >= 240000)
                {
                    resetTotalTransactions();

                    isMessphase         = true;
                    isEinschwingphase   = false;

                    System.out.println("LoadDriver " + name + ": Messphase wurde gestartet...");
                }

                executeRandomFunction(con);

                Thread.sleep(50);
            }

            getKontostandPrepared.close();
            updateBrancheBalancePrepared.close();
            updateTellerBalancePrepared.close();
            updateAccountBalancePrepared.close();
            insertTransactionIntoHistoryPrepared.close();
            getAnzahlEinzahlungPrepared.close();
            con.close();

            System.out.println("LoadDriver " + name + ": Finished Benchmarking...");
        }
        catch(InterruptedException | SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void resetTotalTransactions()
    {
        totalEinzahlungTransactions         = 0;
        totalKontostandTransactions         = 0;
        totalAnzahlEinzahlungenTransactions = 0;
    }

    private void executeRandomFunction(Connection con) throws SQLException
    {
        double random = Math.random();

        if(random < 0.15)
        {
            getAnzahlEinzahlung(getRandomInt(10000));

            totalAnzahlEinzahlungenTransactions++;
        }
        else if(random < 0.5)
        {
            getKontostand(getRandomInt(10000000));

            totalKontostandTransactions++;
        }
        else
        {
            insertEinzahlung(getRandomInt(10000000), getRandomInt(1000), getRandomInt(100), getRandomInt(10000));

            totalEinzahlungTransactions++;
        }

        con.commit();
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

            getKontostandPrepared                   = con.prepareStatement("SELECT balance FROM ACCOUNTS WHERE ACCID = ?");
            updateBrancheBalancePrepared            = con.prepareStatement("UPDATE BRANCHES SET balance = balance + ? WHERE BRANCHID = ?");
            updateTellerBalancePrepared             = con.prepareStatement("UPDATE TELLERS  SET balance = balance + ? WHERE TELLERID = ?");
            updateAccountBalancePrepared            = con.prepareStatement("UPDATE ACCOUNTS SET balance = balance + ? WHERE ACCID    = ?");
            insertTransactionIntoHistoryPrepared    = con.prepareStatement("INSERT INTO HISTORY (ACCID, TELLERID, DELTA, BRANCHID, ACCBALANCE, CMMNT) VALUES(?, ?, ?, ?, ?, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')");
            getAnzahlEinzahlungPrepared             = con.prepareStatement("SELECT COUNT(*) FROM history WHERE delta = ?");

            con.setAutoCommit(false);

            System.out.println("Kontostand Account 5: " + getKontostand(5));

            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(5, 1, 1, 500));
            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(5, 1, 1, 500));
            System.out.println("neuer Kontostand Account 5: " + insertEinzahlung(5, 1, 1, 500));

            System.out.println("Anzahl Einzahlung 500€: " + getAnzahlEinzahlung(500));

            System.out.println("Kontostand Account 5: " + getKontostand(5));

            getKontostandPrepared.close();
            updateBrancheBalancePrepared.close();
            updateTellerBalancePrepared.close();
            updateAccountBalancePrepared.close();
            insertTransactionIntoHistoryPrepared.close();
            getAnzahlEinzahlungPrepared.close();

            con.close();
        }
        catch (ClassNotFoundException | SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private int getRandomInt(int max)
    {
        return random.nextInt(max) + 1;
    }

    private int getKontostand(int ACCID) throws SQLException
    {
        this.getKontostandPrepared.setInt(1, ACCID);
        ResultSet resultSet = this.getKontostandPrepared.executeQuery();

        if(resultSet.next())
        {
            return resultSet.getInt(1);
        }
        return -1;
    }

    private int insertEinzahlung(int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException
    {
        this.updateBrancheBalancePrepared           .setInt(1, DELTA);
        this.updateBrancheBalancePrepared           .setInt(2, BRANCHID);

        this.updateTellerBalancePrepared            .setInt(1, DELTA);
        this.updateTellerBalancePrepared            .setInt(2, TELLERID);

        this.updateAccountBalancePrepared           .setInt(1, DELTA);
        this.updateAccountBalancePrepared           .setInt(2, ACCID);

        this.updateBrancheBalancePrepared           .execute();
        this.updateTellerBalancePrepared            .execute();
        this.updateAccountBalancePrepared           .execute();

        int kontostand = getKontostand(ACCID);

        this.insertTransactionIntoHistoryPrepared   .setInt(1, ACCID);
        this.insertTransactionIntoHistoryPrepared   .setInt(2, TELLERID);
        this.insertTransactionIntoHistoryPrepared   .setInt(3, DELTA);
        this.insertTransactionIntoHistoryPrepared   .setInt(4, BRANCHID);
        this.insertTransactionIntoHistoryPrepared   .setInt(5, kontostand);

        this.insertTransactionIntoHistoryPrepared   .execute();

        return kontostand;
    }

    private int getAnzahlEinzahlung(int DELTA) throws SQLException
    {
        this.getAnzahlEinzahlungPrepared.setInt(1, DELTA);
        ResultSet resultSet = this.getAnzahlEinzahlungPrepared.executeQuery();

        if(resultSet.next())
        {
            return resultSet.getInt(1);
        }

        return 0;
    }
}
