<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Log;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('wells') || ! Schema::hasTable('runs')) {
            Log::error('Source tables "wells" or "runs" do not exist. Cannot populate daily_outcomes.');
            return;
        }

        if (! Schema::hasTable('daily_outcomes')) {
            Log::error('Target table "daily_outcomes" does not exist. Run schema migration first.');
            return;
        }

        Log::warning('Ensure daily_outcomes table is empty before running this migration.');

        $driver = DB::connection()->getDriverName();
        $uuidRegex = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
        $nullUuid = '00000000-0000-0000-0000-000000000000';

        $outcomeTypeExpression = "CASE WHEN w.error_code_id IS NOT NULL THEN 'Error' ELSE 'LIMS' END";
        $wellTypeExpression = "CASE WHEN w.role_alias = 'Patient' THEN 'Patient' ELSE 'Control' END";

        if ($driver === 'pgsql') {
            $outcomeCoalesce = "COALESCE(CAST(w.error_code_id AS VARCHAR), w.lims_status)";
            $outcomeIdExpression = "CASE \n                WHEN {$outcomeCoalesce} ~ '{$uuidRegex}' \n                THEN {$outcomeCoalesce}::uuid \n                ELSE '{$nullUuid}'::uuid \n            END";
            $thermocyclerExpression = "CASE \n                WHEN CAST(r.thermocycler_id AS VARCHAR) ~ '{$uuidRegex}' \n                THEN r.thermocycler_id::uuid \n                ELSE '{$nullUuid}'::uuid \n            END";
            $mixExpression = "CASE \n                WHEN CAST(w.mix_id AS VARCHAR) ~ '{$uuidRegex}' \n                THEN w.mix_id::uuid \n                ELSE '{$nullUuid}'::uuid \n            END";
            $siteExpression = "CASE \n                WHEN CAST(w.site_id AS VARCHAR) ~ '{$uuidRegex}' \n                THEN w.site_id::uuid \n                ELSE '{$nullUuid}'::uuid \n            END";
        } else {
            $outcomeCoalesce = "COALESCE(CAST(w.error_code_id AS CHAR(36)), w.lims_status)";
            $outcomeIdExpression = "CASE \n                WHEN {$outcomeCoalesce} REGEXP '{$uuidRegex}' \n                THEN {$outcomeCoalesce} \n                ELSE '{$nullUuid}' \n            END";
            $thermocyclerExpression = "CASE \n                WHEN CAST(r.thermocycler_id AS CHAR(36)) REGEXP '{$uuidRegex}' \n                THEN CAST(r.thermocycler_id AS CHAR(36)) \n                ELSE '{$nullUuid}' \n            END";
            $mixExpression = "CASE \n                WHEN CAST(w.mix_id AS CHAR(36)) REGEXP '{$uuidRegex}' \n                THEN CAST(w.mix_id AS CHAR(36)) \n                ELSE '{$nullUuid}' \n            END";
            $siteExpression = "CASE \n                WHEN CAST(w.site_id AS CHAR(36)) REGEXP '{$uuidRegex}' \n                THEN CAST(w.site_id AS CHAR(36)) \n                ELSE '{$nullUuid}' \n            END";
        }

        $now = Carbon::now();

        $selectQuery = DB::table('wells as w')
            ->join('runs as r', 'w.run_id', '=', 'r.id')
            ->select(
                DB::raw('w.extraction_date as date'),
                DB::raw('COUNT(*) as count'),
                DB::raw("{$outcomeIdExpression} as outcome_id"),
                DB::raw("{$outcomeTypeExpression} as outcome_type"),
                DB::raw("{$wellTypeExpression} as well_type"),
                DB::raw("{$thermocyclerExpression} as thermocycler_id"),
                DB::raw("{$mixExpression} as mix_id"),
                DB::raw("{$siteExpression} as site_id"),
                DB::raw('? as created_at'),
                DB::raw('? as updated_at')
            )
            ->whereNotNull('w.extraction_date')
            ->where(function ($query) {
                $query->whereNotNull('w.error_code_id')
                    ->orWhereNotNull('w.lims_status');
            })
            ->groupBy(
                DB::raw('w.extraction_date'),
                DB::raw($outcomeIdExpression),
                DB::raw($outcomeTypeExpression),
                DB::raw($wellTypeExpression),
                DB::raw($thermocyclerExpression),
                DB::raw($mixExpression),
                DB::raw($siteExpression)
            )
            ->addBinding($now, 'select')
            ->addBinding($now, 'select');

        Log::info('Preparing to insert aggregated data into daily_outcomes using insertUsing.');

        $insertColumns = [
            'date',
            'count',
            'outcome_id',
            'outcome_type',
            'well_type',
            'thermocycler_id',
            'mix_id',
            'site_id',
            'created_at',
            'updated_at',
        ];

        try {
            Log::debug('Populate Daily Outcomes SQL: ' . $selectQuery->toSql());
            Log::debug('Populate Daily Outcomes Bindings: ', $selectQuery->getBindings());

            $inserted = DB::table('daily_outcomes')->insertUsing($insertColumns, $selectQuery);
            Log::info("daily_outcomes table populated successfully with {$inserted} rows using insertUsing.");
        } catch (\Illuminate\Database\QueryException $e) {
            Log::error('Failed to populate daily_outcomes: ' . $e->getMessage());
            Log::error('Failed SQL: ' . $selectQuery->toSql());
            Log::error('Failed Bindings: ', $selectQuery->getBindings());
            throw $e;
        }
    }

    public function down(): void
    {
        DB::table('daily_outcomes')->truncate();
        Log::info('daily_outcomes table truncated.');
    }
};

