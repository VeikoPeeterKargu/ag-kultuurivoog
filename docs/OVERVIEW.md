# AG Kultuurivoog v3.0 — Project Overview

## 1. Probleem ja lahendus

**Probleem:** Eesti kultuurisündmuste info on killustatud (teatrite kodulehed, portaalid, kalendrid, sotsiaalmeedia). Kasutaja kulutab ebamõistlikult aega, et leida vastus küsimusele “mis täna toimub?”.

**Lahendus:** AG Kultuurivoog on minimalistlik, tekstipõhine ja kiire agregaator, mis koondab sündmused ühele ajajoonele, annab ajafiltrid (täna/7/14/30) ning peidab vaikimisi lasteetendused. Andmetoru sisaldab hügieeni- ja deduplikatsioonikihti, et vältida “müra” (uudised, galeriid) ja duplikaate.

---

## 2. Tehniline arhitektuur (andmevoog)

Süsteem on ehitatud “data-first” põhimõttel — kvaliteet ja stabiilsus enne kasutajaliidest.

### 2.1 Andmekorje (Scrapers)
- Python + `requests` + `BeautifulSoup`
- Allikad (MVP):
  - `teater.ee` (toimiv selgroog)
  - `concert.ee` (MVP-s tuvastati, et pealeht segab uudiseid/sündmusi; rakendatud hügieen)

### 2.2 Deduplikatsioon
Iga sündmus saab `canonical_event_id` (SHA1 räsi), mis põhineb normaliseeritud väljade kombinatsioonil:
- title, date, time, venue, city

See võimaldab UPSERT loogikat ja hoiab andmebaasi puhtana ka korduval korjel.

### 2.3 Andmete hügieen
`cleanup_non_events.py` eemaldab mittevõrdsed kirjed (uudised/galeriid). Eesmärk: API serveerib ainult sündmusi, mitte sisu-uudiseid.

### 2.4 Loogikakiht (SQL Views)
- `v_events_clean` — puhastatud vaade
- `v_events_clean_adults` — sama, kuid peidab lasteetendused (`is_kids_event=0`)

See hoiab API koodi lihtsa ja vähendab reeglite dubleerimist.

### 2.5 API (FastAPI)
Read-only endpointid:
- `/events/today`
- `/events/7days`
- `/events/14days`
- `/events/30days`
- `/events/search?start=YYYY-MM-DD&end=YYYY-MM-DD`

Parameeter:
- `show_kids=false|true` (default false)

### 2.6 UI (Minimalistlik SPA)
- Ühe lehe HTML/JS
- Tekstipõhine tabel
- Ajafilter nupud + “Näita lasteetendusi” lüliti
- Detailvaade (modal) klikiga

---

## 3. Disainiotsused

- **Tekstipõhine pealeht:** maksimaalne kiirus ja loetavus, väldib “plakatiseina”.
- **Kids filter vaikimisi OFF:** täiskasvanu vaade jääb puhas, vajadusel üks klikk.
- **SQLite MVP-s:** kiire arendus ja lihtne deploy; sobib prototüübi ja esialgse avaliku demo jaoks.

---

## 4. Praegused piirangud ja riskid

### 4.1 Ephemeral filesystem (Render tasuta plaan)
SQLite fail võib restartidel kaduda. Seetõttu on vajalik:
- **startup scrape** (täidab DB käivitamisel)
- **tunnine refresh** (hoiab andmed värskena)

### 4.2 Allika kvaliteet (concert.ee)
MVP-s selgus, et pealeht võib segada uudised/galerii ja sündmused. Hügieenikiht eemaldab müra. Pikemas plaanis vajab kontserdiandmete korje täpsemat “event-list” allikat (nt eraldi kalendrivaade / otsinguvaade / struktureeritud feed).

---

## 5. Repositooriumi struktuur

Soovituslik:
- `scrape_teater_ee.py`
- `scrape_concert_ee.py`
- `cleanup_non_events.py`
- `app.py`
- `static/index.html`
- `schema.sql`
- `requirements.txt`
- `Procfile`
- `runtime.txt`
- `docs/OVERVIEW.md`

---

## 6. Käivitamine lokaalselt (MVP)

1) Paigalda sõltuvused:
```bash
pip install -r requirements.txt
```

2) Käivita korje:
```bash
python scrape_teater_ee.py
python scrape_concert_ee.py
python cleanup_non_events.py
```

3) Käivita server:
```bash
uvicorn app:app --reload --port 8000
```

4) Ava:
```
http://localhost:8000
```
