/* ***************************************************************************************************************************************************************

/*************************************************************************************************/
/*                                         Query the data from Bronze Staging Parquet file 
/* --------------------------------------------------------------------------------------------- */
/* Date        | Author              | Description                                               */
/* --------------------------------------------------------------------------------------------- */
/* 01/10/2022  | Ram Baskar          | Query Bronze Staging Parquet file from SSMS               */

/*************************************************************************************************/

SELECT
     TOP 1000 *
FROM
    OPENROWSET(
        --BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_AuditLog/*/*',
		BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_AuditLog/SystemLoadID=1022022090801/dbo_AuditLog_1022022090801.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]