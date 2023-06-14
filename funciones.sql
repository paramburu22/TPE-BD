DROP TABLE IF EXISTS BIRTHS_DEF CASCADE;
DROP TABLE IF EXISTS BIRTHS_TEMP CASCADE;
DROP TABLE IF EXISTS ESTADO CASCADE;
DROP TABLE IF EXISTS ANIO CASCADE;
DROP TABLE IF EXISTS NIVEL_EDUCACION CASCADE;

DROP FUNCTION IF EXISTS distribute();
DROP FUNCTION IF EXISTS ReporteConsolidado(integer);
DROP FUNCTION IF EXISTS ReporteConsolidado_genero(_anio INTEGER, genero CHAR(1));
DROP FUNCTION IF EXISTS ReporteConsolidado_nivel_ed(_anio INTEGER, niv_ed INTEGER);



CREATE TABLE ESTADO (
  nombre_estado varchar(30),
  codigo_estado varchar(2) NOT NULL,
  PRIMARY KEY(codigo_estado),
  UNIQUE(codigo_estado)	
);

CREATE TABLE ANIO (
  anio INTEGER NOT NULL CHECK(anio >= 2016 AND anio <= 2021),
  es_biciesto BOOLEAN,
  PRIMARY KEY(anio)	
);

CREATE TABLE NIVEL_EDUCACION (
  descripcion_ed TEXT,
  nivel_ed INTEGER NOT NULL,
  PRIMARY KEY(nivel_ed),	
  UNIQUE(descripcion_ed)	
);

CREATE TEMPORARY TABLE BIRTHS_TEMP (
  nombre_estado varchar(30),
  codigo_estado varchar(2) NOT NULL,
  anio INTEGER NOT NULL,
  genero varchar(1) NOT NULL,
  descripcion_ed TEXT,
  nivel_ed INTEGER NOT NULL,
  nacimientos INTEGER,
  edad_promedio_madre DECIMAL(3, 1),
  peso_promedio DECIMAL(5, 1) 	
);

CREATE TABLE BIRTHS_DEF (
  codigo_estado varchar(2) NOT NULL,
  anio INTEGER NOT NULL,
  genero varchar(1) NOT NULL,
  nivel_ed INTEGER NOT NULL,
  nacimientos INTEGER,
  edad_promedio_madre DECIMAL(3, 1),
  peso_promedio DECIMAL(5, 1),
  FOREIGN KEY(codigo_estado) REFERENCES ESTADO ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY(anio) REFERENCES ANIO ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY(nivel_ed) REFERENCES NIVEL_EDUCACION(nivel_ed) ON DELETE CASCADE ON UPDATE RESTRICT,
  PRIMARY KEY(anio, codigo_estado, nivel_ed, genero) 	
);

CREATE OR REPLACE FUNCTION distribute() RETURNS trigger AS $distribute$
DECLARE
  es_biciesto BOOLEAN;
BEGIN
  es_biciesto = (new.anio % 4 = 0) AND (new.anio % 100 <> 0);
  IF NOT EXISTS (SELECT 1 FROM anio WHERE anio = NEW.anio) THEN
	INSERT INTO anio VALUES (NEW.anio, es_biciesto);
  END IF;
--   insert into anio values(new.anio, es_biciesto);
  IF NOT EXISTS (SELECT 1 FROM estado WHERE codigo_estado = NEW.codigo_estado) THEN
    INSERT INTO estado VALUES (NEW.nombre_estado, NEW.codigo_estado);
  END IF;
--   insert into estado values(new.nombre_estado, new.codigo_estado);
  IF NOT EXISTS (SELECT 1 FROM nivel_educacion WHERE descripcion_ed = NEW.descripcion_ed) THEN
    INSERT INTO nivel_educacion VALUES (NEW.descripcion_ed, NEW.nivel_ed);
  END IF;
--   insert into nivel_educacion values(new.descripcion_ed, new.nivel_ed);
RETURN NEW;
END;
$distribute$ LANGUAGE plpgsql;

CREATE TRIGGER distribute BEFORE INSERT OR UPDATE ON BIRTHS_TEMP
FOR EACH ROW
EXECUTE PROCEDURE distribute();

COPY BIRTHS_TEMP FROM '/Applications/PostgreSQL 15/us_births_2016_2021.csv' DELIMITER ',' CSV HEADER; 

INSERT INTO BIRTHS_DEF(codigo_estado, anio, genero, nivel_ed, nacimientos, edad_promedio_madre, peso_promedio)
SELECT estado.codigo_estado, anio.anio, births_temp.genero, nivel_educacion.nivel_ed, births_temp.nacimientos, births_temp.edad_promedio_madre, births_temp.peso_promedio
FROM estado, births_temp, anio, nivel_educacion
WHERE estado.codigo_estado = births_temp.codigo_estado
AND estado.nombre_estado = births_temp.nombre_estado
AND anio.anio = births_temp.anio
AND nivel_educacion.nivel_ed = births_temp.nivel_ed
AND nivel_educacion.descripcion_ed = births_temp.descripcion_ed;

CREATE OR REPLACE FUNCTION ReporteConsolidado_nivel_ed(_anio INTEGER, niv_ed INTEGER)
RETURNS VOID AS $$
DECLARE
    categoria TEXT;
    total INTEGER;
    prom_edad INTEGER;
    min_edad INTEGER;
    max_edad INTEGER;
    prom_peso NUMERIC;
    min_peso NUMERIC;
    max_peso NUMERIC;
BEGIN
    SELECT descripcion_ed INTO categoria
    FROM nivel_educacion
    WHERE nivel_ed = niv_ed
	AND nivel_ed > 0;

    SELECT SUM(nacimientos),
           ROUND(AVG(edad_promedio_madre)::numeric, 0),
           ROUND(MIN(edad_promedio_madre)::numeric, 0),
           ROUND(MAX(edad_promedio_madre)::numeric, 0),
           ROUND(AVG(peso_promedio / 1000.0)::numeric, 3),
           ROUND(MIN(peso_promedio / 1000.0)::numeric, 3),
           ROUND(MAX(peso_promedio / 1000.0)::numeric, 3)
    INTO total, prom_edad, min_edad, max_edad, prom_peso, min_peso, max_peso
    FROM BIRTHS_DEF
    WHERE anio = _anio
        AND nivel_ed = niv_ed
		AND nivel_ed > 0;

    RAISE NOTICE '----   Nivel Educacion: %', RPAD(categoria::text, 70, ' ')||RPAD(total::text, 10, ' ')||RPAD(prom_edad::text, 8, ' ')||RPAD(min_edad::text, 8, ' ')||RPAD(max_edad::text, 8, ' ')||RPAD(prom_peso::text, 11, ' ')||RPAD(min_peso::text, 11, ' ')||RPAD(max_peso::text, 11, ' ');

    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ReporteConsolidado_genero(_anio INTEGER, genero_item CHAR(1))
RETURNS VOID AS $$
DECLARE
    total INTEGER;
    prom_edad FLOAT;
    min_edad INTEGER;
    max_edad INTEGER;
    prom_peso FLOAT;
    min_peso FLOAT;
    max_peso FLOAT;
BEGIN
    SELECT
        SUM(nacimientos) INTO total
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
        ROUND(AVG(edad_promedio_madre)::numeric, 0) INTO prom_edad
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
        ROUND(MIN(edad_promedio_madre)::numeric,0) INTO min_edad
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
        ROUND(MAX(edad_promedio_madre)::numeric,0) INTO max_edad
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
        ROUND(AVG(peso_promedio / 1000.0)::numeric, 3) INTO prom_peso
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
       ROUND(MIN(peso_promedio / 1000.0)::numeric, 3) INTO min_peso
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    SELECT
       ROUND(MAX(peso_promedio / 1000.0)::numeric, 3) INTO max_peso
    FROM BIRTHS_DEF
    WHERE anio = _anio AND genero = genero_item;

    RAISE NOTICE '----   Gender: %', RPAD(genero_item::text, 79, ' ')||RPAD(total::text, 10, ' ')||RPAD(prom_edad::text, 8, ' ')||RPAD(min_edad::text, 8, ' ')||RPAD(max_edad::text, 8, ' ')||RPAD(prom_peso::text, 11, ' ')||RPAD(min_peso::text, 11, ' ')||RPAD(max_peso::text, 11, ' ');

    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ReporteConsolidado(cant_anios INTEGER)
RETURNS VOID AS $$
DECLARE
    anio_inicio INTEGER := 2016;
    anio_fin INTEGER := anio_inicio + cant_anios - 1;
    i INTEGER;
	anio_flag BOOL := TRUE;
    estado_item estado%ROWTYPE;
    categoria TEXT;
    genero CHAR(1);
    total INTEGER;
    prom_edad FLOAT;
    min_edad FLOAT;
    max_edad FLOAT;
    prom_peso FLOAT;
    min_peso FLOAT;
    max_peso FLOAT;
    niv_ed nivel_educacion%ROWTYPE;
BEGIN
	IF anio_fin > 2021 THEN
		anio_fin := 2021;
	END IF;
	RAISE NOTICE '========================================================================================================================================================================';
	RAISE NOTICE '=========================================================================CONSOLIDATED BIRTH REPORT======================================================================';
	RAISE NOTICE 'Year===Category========================================================================================Total=====AvgAge==MinAge==MaxAge==AvgWeight==MinWeight==MaxWeight';
	RAISE NOTICE '------------------------------------------------------------------------------------------------------------------------------------------------------------------------';
    FOR i IN anio_inicio..anio_fin LOOP
        FOR estado_item IN SELECT * FROM estado LOOP
            SELECT SUM(nacimientos), ROUND(AVG(edad_promedio_madre)::numeric, 0), ROUND(MIN(edad_promedio_madre)::numeric, 0), ROUND(MAX(edad_promedio_madre)::numeric, 0), ROUND(AVG(peso_promedio/1000.0)::numeric, 3), ROUND(MIN(peso_promedio/1000.0)::numeric, 3), ROUND(MAX(peso_promedio/1000.0)::numeric, 3)
            INTO total, prom_edad, min_edad, max_edad, prom_peso, min_peso, max_peso
            FROM BIRTHS_DEF
            WHERE anio = i AND codigo_estado = estado_item.codigo_estado
            GROUP BY codigo_estado;

            IF total > 200000 THEN
				IF anio_flag = TRUE
				THEN
					RAISE NOTICE '%   State: %', i, RPAD(estado_item.nombre_estado::text, 80, ' ')||RPAD(total::text, 10, ' ')||RPAD(prom_edad::text, 8, ' ')||RPAD(min_edad::text, 8, ' ')||RPAD(max_edad::text, 8, ' ')||RPAD(prom_peso::text, 11, ' ')||RPAD(min_peso::text, 11, ' ')||RPAD(max_peso::text, 11, ' ');
					anio_flag := FALSE;
				ELSE
					RAISE NOTICE '----   State: %', RPAD(estado_item.nombre_estado::text, 80, ' ')||RPAD(total::text, 10, ' ')||RPAD(prom_edad::text, 8, ' ')||RPAD(min_edad::text, 8, ' ')||RPAD(max_edad::text, 8, ' ')||RPAD(prom_peso::text, 11, ' ')||RPAD(min_peso::text, 11, ' ')||RPAD(max_peso::text, 11, ' ');
				END IF;
            END IF;

        END LOOP;

        PERFORM ReporteConsolidado_genero(i,'M');
        PERFORM ReporteConsolidado_genero(i,'F');

        FOR niv_ed IN SELECT * FROM nivel_educacion WHERE nivel_ed != -9 LOOP
            PERFORM ReporteConsolidado_nivel_ed(i, niv_ed.nivel_ed);
        END LOOP;

        SELECT SUM(nacimientos), ROUND(AVG(edad_promedio_madre)::numeric, 0), ROUND(MIN(edad_promedio_madre)::numeric, 0), ROUND(MAX(edad_promedio_madre)::numeric, 0), ROUND(AVG(peso_promedio/1000.0)::numeric, 3), ROUND(MIN(peso_promedio/1000.0)::numeric, 3), ROUND(MAX(peso_promedio/1000.0)::numeric, 3)
        INTO total, prom_edad, min_edad, max_edad, prom_peso, min_peso, max_peso
        FROM BIRTHS_DEF
        WHERE anio = i;

        RAISE NOTICE '--------------------------------------------------------------------------------------------- %',RPAD(total::text, 10, ' ')||RPAD(prom_edad::text, 8, ' ')||RPAD(min_edad::text, 8, ' ')||RPAD(max_edad::text, 8, ' ')||RPAD(prom_peso::text, 11, ' ')||RPAD(min_peso::text, 11, ' ')||RPAD(max_peso::text, 11, ' ');
        RAISE NOTICE '========================================================================================================================================================================';
		anio_flag := TRUE;
    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql;

--DO $$
--BEGIN
--PERFORM ReporteConsolidado(7);
--END;
--$$ LANGUAGE plpgsql
