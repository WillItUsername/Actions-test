-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SPAnvendelse_M1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create model schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_BrugeraktivitetGruppering
-- MAGIC **Forfatter:** Esben Tvergaard (ETVE) <br>
-- MAGIC **Beskrivelse:** Dimensiontabellen indeholder information om gruppering af brugeraktivitet. 'Signal Grouper' er designet ud fra hvordan det gøres i EPIC's Signal-værktøj.<br>
-- MAGIC **Noter:**
-- MAGIC
-- MAGIC **Change Log:**
-- MAGIC | Ver.   | Dato       | Forfatter | Beskrivelse |
-- MAGIC | ------ | ---------- | --------- | ----------- |
-- MAGIC | 1.0    | 06-06-2023 | ETVE0004  | Oprettelse af tabel |
-- MAGIC | 2.0    | 06-06-2023 | ETVE0004  | Test - Nu prøver vi at skrive en rigtig rigtig rigtig lang beskrivelse for at se hvordan tabellen renderer, når der er meget tekst. Ser det fint ud så er det muligvis en gunstig vej at gå med en changelog i en .MD celle frem for i SQL'en. |

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_BrugeraktivitetGruppering`
COMMENT 
    'Dimensiontabellen indeholder information om gruppering af brugeraktivitet. \'Signal Grouper\' er designet ud fra hvordan det gøres i EPIC\'s Signal-værktøj.'
AS
SELECT 
    UserActionLogGroupKey, 
    NAME, 
    CASE
        WHEN Name in ( 'In Basket', 'Kommunikationshåndtering', 'Fælleskommunikation' ) THEN
            'In Basket'
        WHEN Name in ( 'Best./ord.-styring', 'Medicinafstemning', 'Hjemmemedicin' ) THEN
            'Bestillinger'
        WHEN Name in ( 'Notater', 'Diagnoseliste', 'CAVE', 'Historik', 'Opgaveliste', 'Patientplan', 'Flowsheets', 
                        'Vurderingsskemaer', 'Disposition', 'Patientvejledning', 'Kodeangivelse', 'Dok.', 
                        'Lægemiddeladministration', 'Proceduremæssigt arbejdsområde', 'Hændelser'
                    ) THEN
            'Notater'
        WHEN Name in ( 'Patientrapporter', 'Vis journal', 'Synopsis', 'Resultatgennemgang', 
                        'Sundhedsfremme og sygdomsforebyggelse', 'AKA-udredning', 'Storyboard'
                    ) THEN
            'Klinisk gennemgang'
        WHEN Name in ( 'Tidsplan', 'Patientliste', 'Sporing' ) THEN
            'Tidsplan'
        WHEN Name in ( 'Navigatorer' ) THEN
            'Andre navigatorer'
        ELSE
            'Andet' -- ('Dashboards', 'Reporting Workbench', 'SlicerDicer', 'Andet')
    END `Signal Grouper`
FROM
    dap_p_bronze.caboodle.reportingx___UserActionLogGroupDim;
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_Dato

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_Dato`
COMMENT 
    'Dato tabel. Tilpasset til PBI Importmodeller, og inkluderer kun datoer fra 2016 til 5 år ud i fremtiden samt unknown rækker (-1, -2, -3).'
AS
SELECT
    Datedim.DateKey, 
    -- Bruges til relationer
    Datedim.DateValue AS Dato,
    -- Bruges som formatteringsnøgle under "Mark AS Datetable"
    Datedim.`DayOfWeek` AS Ugedag, 
    Datedim.DayOfWeekAbbreviation AS `Ugedag fork.`, 
    Datedim.IndexDayOfWeek AS `Ugedag Numerisk`, 
    Datedim.`DayOfMonth` AS `Dag på Måneden`, 
    Datedim.DayOfYear AS `Dag på Året`, 
    Datedim.Weekend AS Weekend, 
    Datedim.IsHoliday AS Feriedag, 
    Datedim.IsoWeekNumber AS Uge, 
    Datedim.IsoWeekYear AS `Uge og År`, 
    Datedim.WeekStartDate AS `Ugens Startdato`, 
    Datedim.WeekEndDate AS `Ugens Slutdato`, 
    Datedim.MonthName AS `Måned`, 
    Datedim.MonthNameAbbreviation AS `Måned fork.`, 
    Datedim.MonthNumber AS `Månedsnummer`, 
    Datedim.MonthYear AS `Månedsnummer og År`, 
    Datedim.FormattedMonthYear AS `Måned og År`, 
    Datedim.MonthStartDate AS `Månedens Startdato`, 
    Datedim.MonthEndDate AS `Månedens Slutdato`, 
    concat(Datedim.QuarterNumber, '. Kvartal') AS Kvartal, 
    Datedim.QuarterYear AS `Kvartal og År`, 
    Datedim.QuarterStartDate AS `Kvartalets Startdato`, 
    Datedim.QuarterEndDate AS `Kvartalets Slutdato`, 
    Datedim.`Year` AS `År`, 
    Datedim.IsoYearWeek AS `År og Uge`, 
    Datedim.YearMonth AS `År og Månedsnummer`, 
    Datedim.YearFormattedMonth AS `År og Måned`, 
    Datedim.YearFormattedQuarter AS `År og Kvartal`, 
    Datedim.YearStartDate AS `Årets Startdato`, 
    Datedim.YearEndDate AS `Årets Slutdato`, 
    Datedim.PreviousYearDate AS `Dato Sidste År`, 
    Datedim.NextYearDate AS `Dato Næste År`, 
    substring(Datedim.IsoYearWeek, 0, CHARINDEX(' ', Datedim.IsoYearWeek)) AS `År ISO`
FROM 
    dap_p_bronze.caboodle.reportingx___DateDim as DateDim
WHERE 
    Datedim.`Year` BETWEEN 2016 AND (year(current_timestamp()) + 5) 
    -- Dates FROM the year of the initial implementation of SP to five years in the future. Adjusted to import models i Power BI.
    -- OR DateKey < 0 --Include unknown records if there are unknown dates in fact-tables.;
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_Kliniker

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_Kliniker`
COMMENT 
    'Dimensiontabellen indeholder information om kliniske medarbejdere'
AS

/*
Noter: 
- Gruppering af medarbejdere i hhv. læger og sygeplejersker er godkendt af PO JBRE.
Change Log:
*/

SELECT 
    ProviderDim.DurableKey AS ProviderDurableKey, 
    ProviderDim.Name, 
    ProviderDim.PrimaryDepartment, 
    ProviderDim.Type, 
    ProviderDim.PrimarySpecialty, 
    ProviderDim.EmployeeDurableKey_X, 
    ProviderDim.PrimaryDepartmentKey_X,
    CASE
        WHEN ProviderDim.Type In (
                                    'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 
                                    'Børne- og ungdomspsykiater', 'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 
                                    'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
                                    'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 
                                    'Oftalmolog', 'Onkolog', 'Optometrist', 'Overlæge, psykiatri', 'Patolog', 
                                    'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 'Specialtandlæge i Ortodonti', 
                                    'Tandlægestuderende'
                                ) THEN
            'Læge'
        WHEN ProviderDim.Type In (
                                    'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 
                                    'Anæstesisygeplejerske under oplæring', 'Diabetessygeplejerske', 
                                    'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
                                    'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 
                                    'Operationssygeplejerske', 'Opvågningssygeplejerske', 'Sårsygeplejerske', 
                                    'Sedationssygeplejerske', 'Socialsygeplejerske', 'Stomisygeplejerske', 
                                    'Sygeplejerske', 'Sygeplejestuderende'
                                ) THEN
            'Sygeplejerske'
        ELSE
            'Anden faggruppe'
    END `Faggruppe Gruppering`
FROM 
    dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
WHERE ProviderDim.IsCurrent = TRUE -- Fjern historiske records i ProviderDim.
    AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
    AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
    AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
    AND ProviderDim.`Type` -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
        IN ( -- Læger
        'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
        'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
        'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
        'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
        'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
        -- Sygeplejersker
        'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
        'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
        'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
        'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
        'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
    )
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_KlinikerVisualisering

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_KlinikerVisualisering`
COMMENT 
    'Dimensiontabellen indeholder information om kliniske medarbejdere'
AS

/*
Noter: 
- Denne forespørgsel bruges til en specifik visualisering i en Power BI rapport.
*/

SELECT 
  ProviderDim.Name, 
  ProviderDim.PrimaryDepartmentKey_X
FROM 
  dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
WHERE 
  ProviderDim.IsCurrent = True -- Fjern historiske records i ProviderDim.
  AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
  AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
  AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
  AND ProviderDim.Type -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
    IN (    -- Læger
        'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
        'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
        'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
        'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
        'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
        -- Sygeplejersker
        'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
        'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
        'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
        'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
        'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
      )
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_Organisation

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_Organisation`
COMMENT 
    'Information om organisationshierakiet'
AS

/*
Noter:
*/

SELECT 
  DepartmentDim.DepartmentKey, 
  DepartmentDim.AfsnitName_X, 
  DepartmentDim.OverafdelingName_X
FROM dap_p_bronze.caboodle.reportingx___DepartmentDim as DepartmentDim
WHERE 
  IsAfsnit_X = 1 AND OverafdelingEpicId_X is not null 
  -- Fjern departments som ikke kan mappes til en overafdeling, da tabellen bruges til at lave opgørelser på overafdelingsniveau.
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_Tid

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Dim_Tid`
COMMENT 
    'Dimensiontabellen indeholder information om tidspunkter på dagen'
AS

/*
Noter:
*/

SELECT 
  TimeOfDayDim.TimeOfDayKey, 
  TimeOfDayDim.DisplayString24Hour, 
  TimeOfDayDim.HourNumber
FROM 
  dap_p_bronze.caboodle.reportingx___TimeOfDayDim as TimeOfDayDim;
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact_BestOrd

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Fact_BestOrd`
COMMENT 
    'Facttabellen indeholder information om bestillinger og ordinationer sendt fra Sundhedplatformen. Granulariteten i tabellen er én række per Best./Ord. Der medtages både Best./Ord. fra Medicin (MedicationOrderFact) og Procedurer (ProcedureOrderFact)'
AS

/*
Noter:
- Grupperingen af klinikere i læger og sygeplejersker er godkendt af PO (JBRE).
*/

SELECT 
  ProcedureOrderFactX.ProcedureOrderKey AS OrderKey, 
  ProcedureOrderFactX.EncounterKey, 
  ProcedureOrderFactX.OrderedByEmployeeDurableKey, 
  ProcedureOrderFactX.OrderedDateKey, 
  ProcedureOrderFactX.OrderedInstant, 
  ProcedureOrderFactX.PreferenceList, 
  ProcedureOrderFactX.SetSource, 
  CASE
      WHEN ProcedureOrderFactX.OrderSetEpicId is not null
          OR ProcedureOrderFactX.PreferenceList <> '*Unspecified' then
          'Ja'
      else
          'Nej'
  END AS FromListOrSet, -- Er Procedure Best./Ord.'en afstendt fra SmartSet eller Præferenceliste? (Ja/nej)
  ProcedureOrderFactX.Type, 
  ProcedureOrderFactX._LastUpdatedInstant
FROM 
  dap_p_bronze.caboodle.reportingx___ProcedureOrderFactX as ProcedureOrderFactX
  INNER JOIN dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
    ON ProcedureOrderFactX.OrderedByEmployeeDurableKey = ProviderDim.EmployeeDurableKey_X
    AND ProviderDim.IsCurrent = True -- Fjern historiske records i ProviderDim.
    AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
    AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
    AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
    AND ProviderDim.Type -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
    IN ( -- Læger
            'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
            'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
            'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
            'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
            'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
            -- Sygeplejersker
            'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
            'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
            'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
            'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
            'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
        )
WHERE 
  ProcedureOrderFactX.OrderedInstant < dateadd(day, -3, current_timestamp()) 
  -- Fjern bestillinger og ordinationer som er blevet afsendt indenfor de seneste tre dage. Dagene filtreres fra for at aligne med UserActionLogAactivityHourFact som er forsinket tre dage.
  AND ProcedureOrderFactX.ProcedureOrderKey > 0 -- Fjerner special rows (-1, -2, -3).
  AND ProcedureOrderFactX._IsDeleted = 0 -- Fjerner slettede records.

UNION
SELECT 
  MedicationOrderFact.MedicationOrderKey AS OrderKey, 
  MedicationOrderFact.EncounterKey, 
  MedicationOrderFact.OrderedByEmployeeDurableKey, 
  MedicationOrderFact.OrderedDateKey, 
  MedicationOrderFact.OrderedInstant, 
  MedicationOrderFact.PreferenceList, 
  MedicationOrderFact.SetSource, 
  CASE
    WHEN MedicationOrderFact.OrderSetEpicId is not null
      OR MedicationOrderFact.PreferenceList <> '*Unspecified' then
        'Ja'
      else
        'Nej'
  END AS FromListOrSet, 
  -- Er Medicin Best./Ord.'en afstendt fra SmartSet eller Præferenceliste? (Ja/nej)
  'Medication' Type, 
  MedicationOrderFact._LastUpdatedInstant
FROM 
  dap_p_bronze.caboodle.reportingx___MedicationOrderFact as MedicationOrderFact
  INNER JOIN dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
    ON MedicationOrderFact.OrderedByEmployeeDurableKey = ProviderDim.EmployeeDurableKey_X
        AND ProviderDim.IsCurrent = True -- Fjern historiske records i ProviderDim.
        AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
        AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
        AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
        AND ProviderDim.Type -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
    IN ( -- Læger
            'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
            'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
            'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
            'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
            'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
            -- Sygeplejersker
            'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
            'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
            'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
            'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
            'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
        )
WHERE 
  MedicationOrderFact.OrderedInstant < dateadd(day, -3, current_timestamp()) 
  -- Fjern bestillinger og ordinationer som er blevet afsendt indenfor de seneste tre dage. Dagene filtreres fra for at aligne med UserActionLogAactivityHourFact som er forsinket tre dage.
  AND MedicationOrderFact.MedicationOrderKey > 0 -- Fjerner special rows (-1, -2, -3).
  AND MedicationOrderFact._IsDeleted = 0 -- Fjerner slettede records.
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact_Brugeraktivitet

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Fact_Brugeraktivitet`
COMMENT 
    'Facttabellen Indeholder information om medarbejderens aktive tid i Sundhedsplatformen. Branulariteten er en række per medarbejder per påbegyndt time. Tabellens primære entitet er brugerens aktive tid, og tager udgangspunkt i kernemodellen UserActionLogActivityHourFact.'
AS

/*
Noter:
- Grupperingen af klinikere i læger og sygeplejersker er godkendt af PO (JBRE).
*/

SELECT 
  UserActionLogActivityHourFact.ActiveSeconds, 
  UserActionLogActivityHourFact.DateKey, 
  UserActionLogActivityHourFact.EmployeeDurableKey, 
  UserActionLogActivityHourFact.EncounterKey, 
  UserActionLogActivityHourFact.Instant, 
  UserActionLogActivityHourFact.TimeOfDayKey, 
  UserActionLogActivityHourFact.UserActionLogGroupKey, 
  UserActionLogActivityHourFact.WorkspaceSubkindGroup_X, 
  UserActionLogActivityHourFact._LastUpdatedInstant, 
  EncounterFact.Type
FROM 
  dap_p_bronze.caboodle.reportingx___UserActionLogActivityHourFact as UserActionLogActivityHourFact
  INNER JOIN dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
    ON UserActionLogActivityHourFact.EmployeeDurableKey = ProviderDim.EmployeeDurableKey_X
    AND ProviderDim.IsCurrent = True -- Fjern historiske records i ProviderDim.
    AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
    AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
    AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
    AND ProviderDim.Type -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
    IN ( -- Læger
            'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
            'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
            'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
            'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
            'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
            -- Sygeplejersker
            'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
            'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
            'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
            'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
            'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
        )
  LEFT JOIN dap_p_bronze.caboodle.reportingx___EncounterFact as EncounterFact
    ON UserActionLogActivityHourFact.EncounterKey = EncounterFact.EncounterKey
WHERE 
  UserActionLogActivityHourFact.UserActionLogActivityHourKey > 0 -- Fjerner special rows (-1, -2, -3). 
  AND UserActionLogActivityHourFact._IsDeleted = 0 -- Fjerner slettede records.
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact_KliniskNotatBidrag

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SPAnvendelse_M1.`Fact_KliniskNotatBidrag`
COMMENT 
    'Facttabellen indeholder information om kliniske notater og de individuelle bidrag til disse notater. Granulariteten er en række per notatbidrag. Et notatbidrag er en tekstuel tilføjelse til et notat.'
AS

/*
Noter:
*/

SELECT 
  ClinicalNoteFact.ClinicalNoteKey, 
  ClinicalNoteFact.AuthoringEmployeeDurableKey, 
  ClinicalNoteContributionFactX.EmployeeDurableKey AS ContributingEmployeeDurableKey, 
  --ClinicalNoteFact.AuthoringProviderDurableKey, 
  --ClinicalNoteFact.EncounterKey, 
  ClinicalNoteFact.CreationDateKey, 
  ClinicalNoteFact.CreationInstant, 
  --ClinicalNoteFact.Status, 
  ClinicalNoteFact.Type, 
  --ClinicalNoteFact.AuthorType, 
  ClinicalNoteFact.LengthOfNote_X, 
  ClinicalNoteContributionFactX.NumberOfCharacters, 
  ClinicalNoteContributionFactX.ContributionMethod, 
  ClinicalNoteContributionFactX.ContributionMethodGroup, 
  ClinicalNoteContributionFactX._LastUpdatedInstant
FROM 
  dap_p_bronze.caboodle.reportingx___ClinicalNoteFact as ClinicalNoteFact
  INNER JOIN dap_p_bronze.caboodle.reportingx___ClinicalNoteContributionFactX as ClinicalNoteContributionFactX
    ON ClinicalNoteContributionFactX.ClinicalNoteKey = ClinicalNoteFact.ClinicalNoteKey
  INNER JOIN dap_p_bronze.caboodle.reportingx___ProviderDim as ProviderDim
    ON ClinicalNoteContributionFactX.EmployeeDurableKey = ProviderDim.EmployeeDurableKey_X
        AND ProviderDim.IsCurrent = True -- Fjern historiske records i ProviderDim.
        AND ProviderDim.Status_X = 'Aktiv' -- Fjern klinikere (Providers), som ikke er registreret med status aktiv.
        AND ProviderDim.EmployeeDurableKey_X > 0 -- Fjern klinikere (Providers), som ikke har en EMP record.
        AND ProviderDim.PrimaryDepartmentKey_X > 0 -- Fjern aktivitet fra klinikere (Providers), som ikke har registreret en primær afdeling.
        AND ProviderDim.Type -- Fjern kliniere som ikke er læger eller sygeplejersker. Categorization validated by Product Owner (JBRE).
    IN ( -- Læger
            'Anæstesilæge under oplæring', 'Afdelingslæge, psykiatri', 'Anæstesiolog', 'Børne- og ungdomspsykiater', 
            'Gastroenterolog', 'Gynækolog', 'Kæbekirurg', 'Kardiolog', 'Kirurg', 'Læge', 'Medicinstuderende', 
            'Medicinstuderende-Lægevikar', 'Neonatolog', 'Neurolog', 'Obstetriker', 'Oftalmolog', 'Onkolog', 
            'Optometrist', 'Overlæge, psykiatri', 'Patolog', 'Pædiater', 'Psykiater', 'Radiolog', 'Tandlæge', 
            'Specialtandlæge i Ortodonti', 'Tandlægestuderende', 
            -- Sygeplejersker
            'AKA-behandlersygeplejerske', 'Anæstesisygeplejerske', 'Anæstesisygeplejerske under oplæring', 
            'Diabetessygeplejerske', 'Endoskopisygeplejerske', 'Følge-hjem sygeplejerske', 'Hygiejnesygeplejerske', 
            'Intensivsygeplejerske', 'Koloskoperende sygeplejerske', 'Operationssygeplejerske', 
            'Opvågningssygeplejerske', 'Sårsygeplejerske', 'Sedationssygeplejerske', 'Socialsygeplejerske', 
            'Stomisygeplejerske', 'Sygeplejerske', 'Sygeplejestuderende'
        )
WHERE 
  ClinicalNoteFact.CreationInstant < dateadd(day, -3, current_timestamp()) -- Fjern notater som er blevet oprettet indenfor de seneste tre dage. Dagene filtreres fra for at aligne med UserActionLogActivityHourFact som er forsinket tre dage.
  AND ClinicalNoteFact.ClinicalNoteKey > 0 -- Fjerner special rows (-1, -2, -3). 
  AND ClinicalNoteFact._IsDeleted = 0 -- Fjerner slettede records.;
;

-- COMMAND ----------

-- MAGIC %md ## Medarbejdertabel 

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SikkerhedSP_RLS.`RLS_Medarbejdertabel`

COMMENT 
 ' Sikkerhedstabel som skal bruges til at filtrere indhold i Power BI modeller baseret på medarbejderens organisatoriske rettigheder i Sundhedplatformen.Granulariteten er en række per User Principal Name (UPN).'
AS

    
/*-------------------------------------------------------------
Navn: RLS_Medarbejdertabel
Forfatter: Tanya Jarrett (TJAR) og Esben Tvergaard (ETVE)
Noter:
Change Log:
---------------------------------------------------------------
Ver. | Dato`DD-MM-YYYY` | Forfatter | Beskrivelse
1.0  | 12-04-2023       | TJAR0005  | Oprettelse af tabel
2.0  | 05-06-2023       | ETVE0004  | Kommentering og dokumentation af tabel
3.0  | 05-06-2023       | ETVE0004  | Ændring af UPN streg for brugere fra Region Sjælland fra @regionsjaelland.dk til @regsj.dk.
*/ 
---------------------------------------------------------------
-- Finder UPN for brugere fra Region Sjælland
    SELECT concat(left(EmployeeSetDimX.Name, charindex('@', EmployeeSetDimX.Name)), 'regsj.dk') AS Name, 
        AuthorizedHeadDepartmentComboKey_X, 
        AuthorizedHospitalAreaComboKey_X, 
        AuthorizedRegionComboKey_X
    FROM dap_p_bronze.caboodle.reportingx___EmployeeSetDimX as EmployeeSetDimX
        INNER JOIN dap_p_bronze.caboodle.reportingx___EmployeeDim as EmployeeDim
            ON EmployeeSetDimX.EmployeeDurableKey = EmployeeDim.DurableKey    
            AND IsCurrent = 1     
            AND UserStatusName_X = 'Aktiv'
    WHERE EmployeeDurableKey > 0 
            AND AuthorizedHeadDepartmentComboKey_X > 0
            AND Type = 'UPN'
            AND EmployeeSetDimX.Name like '%REGIONSJAELLAND.DK'
UNION 
-- ???
    SELECT EmployeeInformation.email, 
        AuthorizedHeadDepartmentComboKey_X, 
        AuthorizedHospitalAreaComboKey_X, 
        AuthorizedRegionComboKey_X
    FROM dap_p_bronze.caboodle.reportingx___EmployeeSetDimX as EmployeeSetDimX
        INNER JOIN dap_p_bronze.caboodle.reportingx___EmployeeDim as EmployeeDim
            ON EmployeeSetDimX.EmployeeDurableKey = EmployeeDim.DurableKey    
                AND IsCurrent = 1     
                AND UserStatusName_X = 'Aktiv'
        INNER JOIN dap_p_bronze.cbas.EmployeeInformation
            ON dap_p_bronze.cbas.EmployeeInformation.AdministrativtBrugernavn = EmployeeDim.SystemLogin_X
    WHERE EmployeeDurableKey > 0
            AND AuthorizedHeadDepartmentComboKey_X > 0
            AND Type = 'UPN'
UNION
-- ???
 SELECT dap_p_bronze.cbas.EmployeeInformation.email, 
    AuthorizedHeadDepartmentComboKey_X, 
    AuthorizedHospitalAreaComboKey_X, 
    AuthorizedRegionComboKey_X
 FROM dap_p_bronze.caboodle.reportingx___EmployeeSetDimX as EmployeeSetDimX
    INNER JOIN dap_p_bronze.cbas.EmployeeInformation
        ON right( EmployeeSetDimX.Name, 8) = dap_p_bronze.cbas.EmployeeInformation.Brugernavn
    INNER JOIN dap_p_bronze.caboodle.reportingx___EmployeeDim as EmployeeDim
        ON EmployeeSetDimX.EmployeeDurableKey = EmployeeDim.DurableKey    
            AND IsCurrent = 1     
            AND UserStatusName_X = 'Aktiv'
    WHERE EmployeeSetDimX.Type = 'NT_LOGIN'
        AND EmployeeSetDimX.Name Like'REGIONH\%'
        AND email IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OrganisationMedarbejderBrotabel

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SikkerhedSP_RLS.`RLS_OrganisationMedarbejderBrotabel`
COMMENT 
 ' Sikkerhedstabel som skal bruges bro mellem Organisationstabellen og Medarbejdertabellen i RLS i PowerBI.'
AS

    
/*-------------------------------------------------------------
Navn: RLS_OrganisationMedarbejderBrotabel
Forfatter: Esben Tvergaard (ETVE)
Noter:
Change Log:
---------------------------------------------------------------
Ver. | Dato`DD-MM-YYYY` | Forfatter | Beskrivelse
1.0  | 07-06-2023       | ETVE0004  | Oprettelse af tabel
*/
---------------------------------------------------------------
SELECT EmployeeAuthorizationComboKey
, DepartmentKey AS HeadDepartmentKey
FROM dap_p_bronze.caboodle.reportingx___EmployeeAuthorizationBridgeX
WHERE EmployeeAuthorizationComboKey > 0
  AND DepartmentKey > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Organisationstabel

-- COMMAND ----------

CREATE OR REPLACE VIEW dap_p_gold_dataenheden_sandbox.SikkerhedSP_RLS.`RLS_Organisationstabel`
COMMENT 
 ' Sikkerhedstabel som skal bruges til at filtrere indhold i Power BI modeller baseret på brugeres organisatoriske rettigheder i Sundhedplatformen.Granulariteten er en række per organisatoriske enhed. Tabellen indeholder data på tre organisatioriske niveauer Overafdeling, Afdeling og Afsnit.'
AS

    
/*-------------------------------------------------------------
Navn: RLS_Organisationstabel
Forfatter: Esben Tvergaard (ETVE) og Sebastian Wetli (SWET)
Noter: Ved implementering af RLS skal HeaddepartmentKey forbindes til MedarbejderOrganisation-Brotabellen, og DepartmentKey skal forbindes til den Fact-
eller Dimensionstabel man ønsker at filtrere i sin model.
Change Log:
---------------------------------------------------------------
Ver. | Dato`DD-MM-YYYY` | Forfatter | Beskrivelse
1.0  | 12-04-2023       | SWET0005  | Oprettelse af tabel
2.0  | 05-06-2023       | ETVE0004  | Kommentering og dokumentation af tabel
*/
---------------------------------------------------------------
-- Finder alle afsnit
SELECT DISTINCT
    DepartmentDim_Overafdeling.DepartmentKey AS HeadDepartmentKey, 
    DepartmentDim_Afsnit.DepartmentKey AS DepartmentKey
FROM dap_p_bronze.caboodle.reportingx___DepartmentDim AS DepartmentDim_Afsnit
    LEFT JOIN dap_p_bronze.caboodle.reportingx___DepartmentDim AS DepartmentDim_Overafdeling
        ON DepartmentDim_Afsnit.OverafdelingEpicId_X = DepartmentDim_Overafdeling.OverafdelingEpicId_X
WHERE DepartmentDim_Afsnit.IsAfsnit_X = 1
      AND DepartmentDim_Overafdeling.IsOverafdeling_X = 1
UNION ALL
-- Finder alle afdelinger
SELECT DISTINCT
    DepartmentDim_Overafdeling.DepartmentKey AS HeadDepartmentKey, 
    DepartmentDim_Afdeling.DepartmentKey AS DepartmentKey
FROM dap_p_bronze.caboodle.reportingx___DepartmentDim AS DepartmentDim_Afdeling
    LEFT JOIN dap_p_bronze.caboodle.reportingx___DepartmentDim AS DepartmentDim_Overafdeling
        ON DepartmentDim_Afdeling.OverafdelingEpicId_X = DepartmentDim_Overafdeling.OverafdelingEpicId_X
WHERE DepartmentDim_Afdeling.IsAfdeling_X = 1
      AND DepartmentDim_Overafdeling.IsOverafdeling_X = 1
UNION ALL
-- Finder alle overafdelinger
SELECT DepartmentDim_Overafdeling.DepartmentKey AS HeadDepartmentKey, 
  DepartmentDim_Overafdeling.DepartmentKey AS DepartmentKey
FROM dap_p_bronze.caboodle.reportingx___DepartmentDim AS DepartmentDim_Overafdeling
WHERE DepartmentDim_Overafdeling.IsOverafdeling_X = 1;

-- COMMAND ----------


