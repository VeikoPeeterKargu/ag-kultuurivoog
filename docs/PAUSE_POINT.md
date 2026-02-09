# Pause Point

## Kus pooleli jäime
Protsess on peatatud faasis, kus kolisime skraperid Renderist GitHub Actionsi peale. Eesmärk oli vältida Renderi IP-aadressilt tulevaid 403 vigu `teater.ee` lehelt.

## Järgmised sammud naasmisel

1. **Kontrolli GitHub Actionsi logisid:**
   - Vaata "Daily Scrape" workflow jooksutusi GitHubis.
   - Kas `scrape_teater_ee.py` sai andmed kätte või andis 403 veateate?

2. **Kui 403 püsib:**
   - See tähendab, et ka GitHubi IP-d on blokeeritud.
   - **Lahendus:** Seadista Self-hosted Runner (lokaalne arvuti, nt Mac Mini või väike VPS).
   - See võimaldab jooksutada GitHub Actions workflow'd sinu enda masinas, millel on "puhas" IP.

3. **Edasiarendused:**
   - Lisa uued andmeallikad (nt Piletilevi, Piletitasku).
   - Täiusta UI-d filtrite ja vaadetega.
