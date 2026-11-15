-- Bootstrap administrator stays separate; only the app credential reaches the app container.
\getenv app_password ORBITA_APP_PASSWORD
create role orbita login nosuperuser nocreaterole createdb password :'app_password';
alter database orbita owner to orbita;
grant all on schema public to orbita;
