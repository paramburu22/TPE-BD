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

COPY BIRTHS_TEMP FROM 'C:\Users\Public\us_births_2016_2021.csv' DELIMITER ',' CSV HEADER; 

INSERT INTO BIRTHS_DEF(codigo_estado, anio, genero, nivel_ed, nacimientos, edad_promedio_madre, peso_promedio)
SELECT estado.codigo_estado, anio.anio, births_temp.genero, nivel_educacion.nivel_ed, births_temp.nacimientos, births_temp.edad_promedio_madre, births_temp.peso_promedio
FROM estado, births_temp, anio, nivel_educacion
WHERE estado.codigo_estado = births_temp.codigo_estado
AND estado.nombre_estado = births_temp.nombre_estado
AND anio.anio = births_temp.anio
AND nivel_educacion.nivel_ed = births_temp.nivel_ed
AND nivel_educacion.descripcion_ed = births_temp.descripcion_ed;