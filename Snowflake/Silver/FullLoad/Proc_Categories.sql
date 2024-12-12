USE SCHEMA DW.SILVER;

CREATE OR REPLACE PROCEDURE SILVER_CATEGORIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    create TABLE if not exists DW.SILVER.CATEGORIES (
    CATEGORY_ID NUMBER(38,0) NOT NULL,
    CATEGORY_NAME VARCHAR(15) NOT NULL,
    DESCRIPTION VARCHAR(16777216),
    DATA_EXTRACAO DATE
    );

    TRUNCATE TABLE DW.SILVER.CATEGORIES;

     -- Inicia uma transação
    BEGIN TRANSACTION;

        -- Insere os dados transformados na tabela DW.SILVER.CATEGORIES
        INSERT INTO DW.SILVER.CATEGORIES (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION, DATA_EXTRACAO)
        SELECT 
            CATEGORY_ID, 
            UPPER(CATEGORY_NAME) AS CATEGORY_NAME, 
            UPPER(COALESCE(DESCRIPTION, 'NA')) AS DESCRIPTION,
            DATA_EXTRACAO
        FROM DW.BRONZE.CATEGORIES;

         -- Finaliza a transação
        COMMIT;

        RETURN 'Dados transferidos e transformados com sucesso';

EXCEPTION
    WHEN OTHER THEN
        -- Em caso de erro, realiza o rollback
        ROLLBACK;
        RETURN 'Erro durante a execução da procedure.';
    
END;
$$;

CALL SILVER_CATEGORIES();