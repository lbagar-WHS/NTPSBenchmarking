package org.example;

import Controller.BenchmarkController;
import Loader.LoadDriver;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main
{
    /* Lokaler Rechner ohne Optimierung
     * +------+------------+---------------------------+-------------------------+
     * |  n   | Laufzeit   | Speicherverbrauch         | Speicherverbrauch (B)   |
     * +------+------------+---------------------------+-------------------------+
     * |  1   | 142s       | branches:  0,008 MB       | branches:  8.000 B      |
     * |      |            | tellers:   0,008 MB       | tellers:   8.000 B      |
     * |      |            | accounts:  10,563 MB      | accounts:  10.563.000 B |
     * |      |            | history:   0,000 MB       | history:   0 B          |
     * +------+------------+---------------------------+-------------------------+
     * | 10   | 898s       | branches:  0,008 MB       | branches:  8.000 B      |
     * |      |            | tellers:   0,016 MB       | tellers:   16.000 B     |
     * |      |            | accounts:  105,578 MB     | accounts:  105.578.000 B|
     * |      |            | history:   0,000 MB       | history:   0 B          |
     * +------+------------+---------------------------+-------------------------+
     *
     * autocommit = false --> 387s
     *
     * batch alle Rows = n=1 -> 21s, n=10 -> Systemarbeitsspeicher überlaufen
     * batch alle 10   Rows, SQL Befehle in ArrayList schreiben  = n=1 -> 9s, n=10 -> OpenJDK 64-Bit Server VM warning: INFO: os::commit_memory(0x00000000ad600000, 55574528, 0) failed; error='Die Auslagerungsdatei ist zu klein, um diesen Vorgang durchzuf�hren' (DOS error/errno=1455)
     * batch alle 10   Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 9s, n=10 -> 98s
     * batch alle 100  Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 7s, n=10 -> 53s
     * batch alle 1000 Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 6s, n=10 -> 51s --> scheint gut Wahl zu sein
     * batch alle 2000 Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 5s, n=10 -> 179s
     * batch alle 1500 Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 7s, n=10 -> 53s
     * batch alle 1250 Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 5s, n=10 -> 118s
     *
     * preparedStatement anstatt createStatement
     * batch alle 1000 Rows, SQL Befehle nicht zwischenspeichern = n=1 -> 2s, n=10 -> 19s --> deutliche Performance Verbesserung
     *
     * optimiertes Programm DB auf lokalem Rechner:
     * n=10 -> 17s
     * n=20 -> 31s
     * n=50 -> 76s
     * optimiertes Programm DB auf LAN Rechner:
     * n=10 -> 20s
     * n=20 -> 35s
     * n=50 -> 80s
     */

    public static void main(String[] args)
    {
        Scanner scanner = new Scanner(System.in);  // Create a Scanner object
        int option;

        do
        {
            System.out.println("Welche der folgenden Operationen soll ausgeführt werden:");
            System.out.println("(1): ntps-Datenbank erstellen");
            System.out.println("(2): Einträge in HISTORY Tabelle löschen");
            System.out.println("(3): Lastentest starten");
            System.out.println("(4): Testdurchlauf");
            System.out.println("(5): Programm beenden");

            System.out.println();

            option = scanner.nextInt();

            switch (option)
            {
                case 1:     initDB(scanner); break;
                case 2:     deleteHistoryEntries(); break;
                case 3:     startBenchmarking(); break;
                case 4:     testBenchmarkingMethod(); break;
                case 5:     System.out.println("Programm wird beendet"); break;
                default:    System.out.println("Falsche Eingabe"); break;
            }
        }
        while (option != 5);

        scanner.close();
    }

    private static void testBenchmarkingMethod()
    {
        LoadDriver driver = new LoadDriver("1", 1);

        driver.testExecuteFunction();
    }

    private static void startBenchmarking()
    {
        LoadDriver.resetTotalTransactions();

        try
        {
            ExecutorService executorService = Executors.newFixedThreadPool(5);

            for (int i = 1; i <= 5; i++)
            {
                executorService.submit(new LoadDriver("" + i, i + 1));
            }

            executorService.shutdown();
            executorService.awaitTermination(11, TimeUnit.MINUTES);

            int totalKontostandTransactions         = LoadDriver.totalKontostandTransactions;
            int totalAnzahlEinzahlungenTransactions = LoadDriver.totalAnzahlEinzahlungenTransactions;
            int totalEinzahlungTransactions         = LoadDriver.totalEinzahlungTransactions;
            int totalTransactions                   = totalKontostandTransactions + totalAnzahlEinzahlungenTransactions + totalEinzahlungTransactions;

            System.out.println();
            System.out.println("alle 5 Benchmarkings wurden abgeschlossen...");
            System.out.println("Anzahl getKontostand:       " + totalKontostandTransactions         + " (" + ((float) totalKontostandTransactions           / totalTransactions) * 100 + "%)");
            System.out.println("Anzahl getAnzahlEinzahlung: " + totalAnzahlEinzahlungenTransactions + " (" + ((float) totalAnzahlEinzahlungenTransactions   / totalTransactions) * 100 + "%)");
            System.out.println("Anzahl insertEinzahlung:    " + totalEinzahlungTransactions         + " (" + ((float) totalEinzahlungTransactions           / totalTransactions) * 100 + "%)");
            System.out.println("Anzahl Transaktionen:       " + totalTransactions);
            System.out.println("Anzahl Transaktionen pro Sekunde pro Thread: " + totalTransactions / 60 / 5 / 5);
            System.out.println();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void deleteHistoryEntries()
    {
        BenchmarkController controller = new BenchmarkController();

        controller.deleteHistoryEntries();
    }

    private static void initDB(Scanner scanner)
    {
        System.out.println("Wert für n: ");

        BenchmarkController controller = new BenchmarkController();

        int n = scanner.nextInt();

        controller.initDB(n);
    }
}