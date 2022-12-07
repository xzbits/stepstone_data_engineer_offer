# Get the table include referenced number, job offer city, contract limitation, company size
analyze_query = """
SELECT arbort.arbeitort_ort, geber.betriebsgroesse, geber.name
FROM 
    stellenangebote stell
    INNER JOIN auftragsdetails auf ON stell.refnr = auf.refnr
    INNER JOIN arbeitort arbort ON auf.refnr = arbort.refnr
    LEFT JOIN arbeitgeber geber on stell.arbeitgeber = geber.name AND auf.branchengruppe = geber.branchengruppe
    WHERE 'Data Engineer' ~ stell.beruf
"""
