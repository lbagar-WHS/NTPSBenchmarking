package org.example;

public class testZeit
{
    public static void main(String args[])
    {
        System.out.println("Start");

        try
        {
            long starttime = System.currentTimeMillis();
            int countTransaktions = 0;
            boolean isEinschwingphase = true;
            boolean isMessphase = false;
            boolean isAusschwingphase = false;

            while ((System.currentTimeMillis() - starttime) <= 600000)
            {
                if (isMessphase && (System.currentTimeMillis() - starttime) >= 540000)
                {
                    isMessphase = false;
                    isAusschwingphase = true;

                    System.out.println("Messphase wurde beendet, Ausschwingphase gestartet...");
                    System.out.println("Anzahl Transaktionen insgesamt: " + countTransaktions);
                    System.out.println("Anzahl Transaktionen pro Sekunde Durchschnitt: " + countTransaktions / 5 / 60);
                }
                else if (isEinschwingphase && (System.currentTimeMillis() - starttime) >= 240000)
                {
                    isMessphase = true;
                    isEinschwingphase = false;

                    System.out.println("Messphase wird gestartet...");
                }

                if (isMessphase)
                {
                    countTransaktions++;
                }

                Thread.sleep(50);
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

    }
}
