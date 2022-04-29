DROP TABLE IF EXISTS datos_cines;
DROP TABLE IF EXISTS datos_museos;
DROP TABLE IF EXISTS datos_bibliotecas;
DROP TABLE IF EXISTS cines;
CREATE TABLE IF NOT EXISTS public.cines (provincia varchar, butacas integer, pantallas integer, espacio_incaa integer);
CREATE TABLE IF NOT EXISTS public.datos_cines (cod_localidad integer, id_provincia integer, id_departamento integer,categoria varchar, provincia varchar, localidad varchar, nombre varchar, domicilio varchar, codigo_postal varchar, numero_de_telefono integer, mail varchar, web varchar);
CREATE TABLE IF NOT EXISTS public.datos_museos (cod_localidad integer, id_provincia integer, id_departamento integer,categoria varchar, provincia varchar, localidad varchar, nombre varchar, domicilio varchar, codigo_postal varchar, numero_de_telefono integer, mail varchar, web varchar);
CREATE TABLE IF NOT EXISTS public.datos_bibliotecas (cod_localidad integer, id_provincia integer, id_departamento integer,categoria varchar, provincia varchar, localidad varchar, nombre varchar, domicilio varchar, codigo_postal varchar, numero_de_telefono integer, mail varchar, web varchar);
