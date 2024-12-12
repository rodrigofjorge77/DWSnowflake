USE SCHEMA DW.GOLD;

CREATE OR REPLACE PROCEDURE DW.GOLD.PROC_DIM_TEMPO(DATA_INICIO DATE, DATA_FIM DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    DATA_ATUAL DATE;
BEGIN

    CREATE TABLE IF NOT EXISTS DW.GOLD.DIM_TEMPO (
        SK_DATA NUMBER AUTOINCREMENT , -- Serial substituído por AUTOINCREMENT
        DATA DATE NOT NULL,
        ANO NUMBER(4,0) NOT NULL, -- Número de 4 dígitos para o ano
        MES NUMBER(2,0) NOT NULL, -- Número de 2 dígitos para o mês
        DIA NUMBER(2,0) NOT NULL, -- Número de 2 dígitos para o dia
        DIA_SEMANA NUMBER(1,0) NOT NULL, -- Número de 1 dígito para o dia da semana
        DIA_ANO NUMBER(3,0) NOT NULL, -- Número de 3 dígitos para o dia do ano
        ANO_BISSEXTO CHAR(1) NOT NULL,
        DIA_UTIL CHAR(1) NOT NULL,
        FIM_SEMANA CHAR(1) NOT NULL,
        FERIADO CHAR(1) NOT NULL,
        PRE_FERIADO CHAR(1) NOT NULL,
        POS_FERIADO CHAR(1) NOT NULL,
        NOME_FERIADO VARCHAR(30),
        NOME_DIA_SEMANA VARCHAR(15) NOT NULL,
        NOME_DIA_SEMANA_ABREV CHAR(3) NOT NULL,
        NOME_MES VARCHAR(15) NOT NULL,
        NOME_MES_ABREV CHAR(3) NOT NULL,
        QUINZENA NUMBER(1,0) NOT NULL, -- Número de 1 dígito para quinzena
        BIMESTRE NUMBER(1,0) NOT NULL, -- Número de 1 dígito para bimestre
        TRIMESTRE NUMBER(1,0) NOT NULL, -- Número de 1 dígito para trimestre
        SEMESTRE NUMBER(1,0) NOT NULL, -- Número de 1 dígito para semestre
        NR_SEMANA_MES NUMBER(1,0) NOT NULL, -- Número de 1 dígito para a semana do mês
        NR_SEMANA_ANO NUMBER(2,0) NOT NULL, -- Número de 2 dígitos para a semana do ano
        DATA_POR_EXTENSO VARCHAR(50) NOT NULL,
        EVENTO VARCHAR(50)
    );

    TRUNCATE TABLE DW.GOLD.DIM_TEMPO;

    -- Inicializa a data atual como a data de início
    DATA_ATUAL := DATA_INICIO;

    -- Inicia uma transação
    BEGIN TRANSACTION;

    -- Loop enquanto a data atual estiver dentro do intervalo
    WHILE ( DATA_ATUAL <= DATA_FIM ) DO
				 	
        INSERT INTO DIM_TEMPO (
            DATA,
            ANO,
            MES,
            DIA,
            DIA_SEMANA,
            DIA_ANO,
            ANO_BISSEXTO,
            DIA_UTIL,
            FIM_SEMANA,
            FERIADO,
            PRE_FERIADO,
            POS_FERIADO,
            NOME_FERIADO,
            NOME_DIA_SEMANA,
            NOME_DIA_SEMANA_ABREV,
            NOME_MES,
            NOME_MES_ABREV,
            QUINZENA,
            BIMESTRE,
            TRIMESTRE,
            SEMESTRE,
            NR_SEMANA_MES,
            NR_SEMANA_ANO,
            DATA_POR_EXTENSO,
            EVENTO
        )
		SELECT :DATA_ATUAL ,
            YEAR(:DATA_ATUAL),
            MONTH(:DATA_ATUAL),
            DAY(:DATA_ATUAL),
            DAYOFWEEK(:DATA_ATUAL),
            DAYOFYEAR(:DATA_ATUAL),
            CASE WHEN MOD(YEAR(:DATA_ATUAL), 4) = 0 AND (MOD(YEAR(:DATA_ATUAL), 100) != 0 OR MOD(YEAR(:DATA_ATUAL), 400) = 0) THEN 'Y' ELSE 'N' END,
            CASE WHEN DAYOFWEEK(:DATA_ATUAL) BETWEEN 2 AND 6 THEN 'Y' ELSE 'N' END,
            CASE WHEN DAYOFWEEK(:DATA_ATUAL) IN (1, 7) THEN 'Y' ELSE 'N' END,
            'N', -- Valor padrão para FERIADO
            'N', -- Valor padrão para PRE_FERIADO
            'N', -- Valor padrão para POS_FERIADO
            NULL, -- NOME_FERIADO, ajustável conforme lista de feriados
            TO_CHAR(:DATA_ATUAL, 'Day'),
            TO_CHAR(:DATA_ATUAL, 'Dy'),
            TO_CHAR(:DATA_ATUAL, 'Month'),
            TO_CHAR(:DATA_ATUAL, 'Mon'),
            CASE WHEN DAY(:DATA_ATUAL) <= 15 THEN 1 ELSE 2 END,
            CEIL(MONTH(:DATA_ATUAL) / 2),
            CEIL(MONTH(:DATA_ATUAL) / 3),
            CEIL(MONTH(:DATA_ATUAL) / 6),
            CEIL(DAY(:DATA_ATUAL) / 7),
            WEEKOFYEAR(:DATA_ATUAL),
            TO_CHAR(:DATA_ATUAL, 'DD "de" Month "de" YYYY'),
            NULL ; -- Valor padrão para EVENTO    */        

        -- Incrementa a data atual em 1 dia
        DATA_ATUAL := DATEADD(DAY, 1, DATA_ATUAL);

    END WHILE;
    
    COMMIT;

    RETURN 'Dados inseridos com sucesso na tabela DIM_TEMPO!';

EXCEPTION
    WHEN OTHER THEN
        -- Em caso de erro, realiza o rollback
        ROLLBACK;
        RETURN 'Erro durante a execução da procedure.';

END;
$$;

CALL DW.GOLD.PROC_DIM_TEMPO('2024-01-01', '2024-01-02');