USE SCHEMA DW.SILVER;

CREATE OR REPLACE PROCEDURE SILVER_TERRITORIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
	
    create TABLE IF NOT EXISTS DW.SILVER.TERRITORIES (
        TERRITORY_ID VARCHAR(20) NOT NULL,
        TERRITORY_DESCRIPTION VARCHAR(60) NOT NULL,
        REGION_ID NUMBER(38,0) NOT NULL,
        DATA_EXTRACAO DATE
    );

	TRUNCATE TABLE DW.SILVER.TERRITORIES;

    -- Inicia uma transação
    BEGIN TRANSACTION;

    -- Insere os dados transformados na tabela DW.SILVER.CATEGORIES
    INSERT INTO DW.SILVER.TERRITORIES (TERRITORY_ID,TERRITORY_DESCRIPTION,REGION_ID,DATA_EXTRACAO)       
    SELECT COALESCE(TERRITORY_ID,-1), 
        COALESCE(TERRITORY_DESCRIPTION,'NA'), 
        COALESCE(REGION_ID,-1), 
        DATA_EXTRACAO
    FROM DW.BRONZE.TERRITORIES;

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

CALL SILVER_TERRITORIES();
