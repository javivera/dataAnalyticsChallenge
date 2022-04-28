DROP TABLE IF EXISTS datos;
DROP TABLE IF EXISTS cines;
CREATE TABLE IF NOT EXISTS public.cines (provincia varchar, butacas integer, pantallas integer, espacio_incaa integer);
CREATE TABLE IF NOT EXISTS public.datos (cod_localidad integer, id_provincia integer, id_departamento integer,categoria varchar, provincia varchar, localidad varchar, nombre varchar, domicilio varchar, codigo_postal varchar, num_telefono integer, mail varchar, web varchar);
