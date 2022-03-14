CREATE TABLE public.dim_hospital (
	hospital_id serial4 NOT NULL,
	hospital_original_id varchar(200) NOT NULL,
	hospital_name varchar(200) NOT NULL,
	start_date timestamp NOT NULL,
	end_date timestamp NULL,
	CONSTRAINT dim_hospital_pk PRIMARY KEY (hospital_id)
);
CREATE UNIQUE INDEX dim_hospital_id_end_date ON public.dim_hospital USING btree (hospital_original_id, ((end_date IS NULL))) WHERE (end_date IS NULL);

CREATE TABLE public.dim_owner (
	owner_id serial4 NOT NULL,
	owner_original_id varchar(200) NOT NULL,
	owner_first_name varchar(200) NULL,
	owner_last_name varchar(200) NULL,
	owner_email varchar(200) NULL,
	owner_address varchar(200) NULL,
	owner_city varchar(200) NULL,
	owner_postcode varchar(200) NULL,
	start_date timestamp NOT NULL,
	end_date timestamp NULL,
	CONSTRAINT dim_owner_pk PRIMARY KEY (owner_id)
);
CREATE UNIQUE INDEX dim_owner_id_end_date ON public.dim_owner USING btree (owner_original_id, ((end_date IS NULL))) WHERE (end_date IS NULL);

CREATE TABLE public.dim_pet (
	pet_id serial4 NOT NULL,
	pet_original_id varchar(200) NOT NULL,
	pet_name varchar(500) NULL,
	pet_species varchar(200) NOT NULL,
	pet_breed varchar(200) NOT NULL,
	pet_dob date NULL,
	start_date timestamp NOT NULL,
	end_date timestamp NULL,
	CONSTRAINT dim_pet_pk PRIMARY KEY (pet_id)
);
CREATE UNIQUE INDEX dim_pet_id_end_date ON public.dim_pet USING btree (pet_original_id, ((end_date IS NULL))) WHERE (end_date IS NULL);

CREATE TABLE public.dim_procedure (
	procedure_id serial4 NOT NULL,
	procedure_original_id varchar(100) NOT NULL,
	procedure_desc varchar(500) NULL,
	start_date timestamp NOT NULL,
	end_date timestamp NULL,
	CONSTRAINT dim_procedure_pk PRIMARY KEY (procedure_id)
);
CREATE UNIQUE INDEX dim_procedure_id_end_date ON public.dim_procedure USING btree (procedure_original_id, ((end_date IS NULL))) WHERE (end_date IS NULL);

CREATE TABLE public.error_vet_visit (
	visit_id int8 NULL,
	visit_start_date text NULL,
	visit_end_date timestamp NULL,
	visit_cost text NULL,
	procedure_code text NULL,
	procedure_desc text NULL,
	hospital_id int8 NULL,
	hospital text NULL,
	owner_id int8 NULL,
	first_name text NULL,
	last_name text NULL,
	email text NULL,
	address text NULL,
	city text NULL,
	postcode text NULL,
	pet_id int8 NULL,
	pet_name text NULL,
	species text NULL,
	breed text NULL,
	pet_dob text NULL,
	file text NULL,
	import_ts timestamp NULL
);


CREATE TABLE public.fact_visit (
	visit_id serial4 NOT NULL,
	visit_original_id varchar(200) NOT NULL,
	visit_start_date date NOT NULL,
	visit_end_date date NOT NULL,
	visit_cost float8 NOT NULL,
	procedure_id int4 NOT NULL,
	hospital_id int4 NOT NULL,
	owner_id int4 NOT NULL,
	pet_id int4 NOT NULL,
	CONSTRAINT fact_visit_pk PRIMARY KEY (visit_id),
	CONSTRAINT fact_visit_fk_hospital FOREIGN KEY (hospital_id) REFERENCES public.dim_hospital(hospital_id),
	CONSTRAINT fact_visit_fk_owner FOREIGN KEY (owner_id) REFERENCES public.dim_owner(owner_id),
	CONSTRAINT fact_visit_fk_pet FOREIGN KEY (pet_id) REFERENCES public.dim_pet(pet_id),
	CONSTRAINT fact_visit_fk_procedure FOREIGN KEY (procedure_id) REFERENCES public.dim_procedure(procedure_id)
);