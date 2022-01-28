CREATE TABLE Datos (
    id SERIAL PRIMARY KEY,
    cod_loc BIGINT,
    idprovincia BIGINT,
    iddepartamento BIGINT,
    categoria VARCHAR(255),
    provincia VARCHAR(255),
    localidad VARCHAR(255),
    nombre VARCHAR(255),
    domicilio VARCHAR(255),
    cp VARCHAR(255),
    telefono VARCHAR(255),
    mail VARCHAR(255),
    web VARCHAR(255)
);