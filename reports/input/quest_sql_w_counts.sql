-- GET CODES TO CHECK QUERIES
select count(id) from wells where wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01' and wells.site_id != '995a4ffa-0ee4-4f55-9d2d-69877dd36d75';

select distinct error_code_id from wells where wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01';
-- to ignore ('937829a2-56a2-4c3a-9761-9d5f5efa4cfc', '937829a2-9af2-42e7-8f96-06ceb4a6e512', '937829a3-aa88-44cf-bbd5-deade616cff5', '98b5395c-97be-4dbd-b185-9a57a25a31ca', '995a530f-2239-4007-80f9-4102b5826ee5', '995a530f-b2f6-423e-9d28-3d512c03a940' ,'995a530f-b2f6-423e-9d28-3d512c03a940', )
select distinct lims_status from wells w ;

-- **DISCREPS** --
-- result changed
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
      and observations.final_cls = observations.dxai_cls
      and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%') -- the 'valid lims' should be from CSV as may be different for Viracor for example.
      and wells.resolution_codes like '%bla%'
      and wells.role_alias like 'Patient' and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'; -- bla is universal for all dbs (user changed classification code)
-- COUNT: 219
      
--ignored error
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
      and observations.final_cls = observations.machine_cls
      and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%')
		and wells.resolution_codes like '%bla%'
		and wells.role_alias like 'Patient' and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01';
-- COUNT: 3728

-- sample repeated
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
	AND  (wells.lims_status not like '%detected%' and wells.lims_status not like '%1500%' or wells.lims_status is null)
	and wells.error_code_id not in ('937829a2-56a2-4c3a-9761-9d5f5efa4cfc', '937829a2-9af2-42e7-8f96-06ceb4a6e512', '937829a3-aa88-44cf-bbd5-deade616cff5', '98b5395c-97be-4dbd-b185-9a57a25a31ca', '995a530f-2239-4007-80f9-4102b5826ee5', '995a530f-b2f6-423e-9d28-3d512c03a940' ,'995a530f-b2f6-423e-9d28-3d512c03a940' )
	and wells.role_alias like 'Patient' and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
-- COUNT: 1411
	
	
-- **Sample (SOP)**
-- sop repeat
	select * from wells where
		wells.role_alias = 'Patient'
		and wells.resolution_codes not like '%bla%' -- must have non bla (curve classification change) resolution code
		and ((wells.lims_status not like '%detected%' and wells.lims_status not like '%1500%') or wells.lims_status is null) -- no valid detected result or no lims at all (no lims = it has error code)
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		and (wells.error_code_id not in ('937829a2-56a2-4c3a-9761-9d5f5efa4cfc', '937829a2-9af2-42e7-8f96-06ceb4a6e512', '937829a3-aa88-44cf-bbd5-deade616cff5', '98b5395c-97be-4dbd-b185-9a57a25a31ca', '995a530f-2239-4007-80f9-4102b5826ee5', '995a530f-b2f6-423e-9d28-3d512c03a940' ,'995a530f-b2f6-423e-9d28-3d512c03a940' )--not in control related issue, and not a classification error (WDCLS). Should use ec join not uuids.
			or wells.error_code_id is null)
-- COUNT: 4,585
		
-- sop unresolved
	select * from wells where
		wells.role_alias = 'Patient'
		and ((wells.resolution_codes not like '%bla%') or wells.resolution_codes is null) -- must have non bla (curve classification change) resolution code
		and wells.lims_status is null
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		and wells.error_code_id not in ('937829a2-56a2-4c3a-9761-9d5f5efa4cfc', '937829a2-9af2-42e7-8f96-06ceb4a6e512', '937829a3-aa88-44cf-bbd5-deade616cff5', '98b5395c-97be-4dbd-b185-9a57a25a31ca', '995a530f-2239-4007-80f9-4102b5826ee5', '995a530f-b2f6-423e-9d28-3d512c03a940' ,'995a530f-b2f6-423e-9d28-3d512c03a940' )
--COUNT: 3868
		
-- sop ignored
		select * from wells where
		wells.role_alias = 'Patient'
		and wells.resolution_codes not like '%bla%' -- must have non bla (curve classification change) resolution code, not bla (and not null)
		and (wells.lims_status like '%detected%' OR wells.lims_status like '%1500%')
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
	--COUNT: 826
		
-- **VALID RESULTS (for summary)**
	--control repeat
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is not null
		and (wells.lims_status is not null or wells.error_code_id is not null) -- set all associated patient results to this repeat result, or has error
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
	--COUNT: 190
		
	--controls error ignored
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is not null -- has a resolution
		and wells.lims_status is null -- no repeats etc
		and wells.error_code_id is null -- no error code
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01';
	--COUNT: 769	
		
	-- control error not resolved
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is null -- no resolution (excludes above two categories)
		and (wells.lims_status is not null or wells.error_code_id is not null) -- well retains error or repeat status
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
	-- COUNT: 1203	
		
-- patients with no warnings or issues
		select * from wells WHERE 
			wells.role_alias = 'Patient'
			and wells.resolution_codes is NULL 
			and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%')
			and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		--COUNT: 102,062
			
-- controls with no warnings or issues
		select * from wells WHERE 
			wells.role_alias != 'Patient'
			and wells.resolution_codes is NULL 
			and wells.lims_status is null -- passed controls have blank lims
			and wells.error_code_id is NULL  -- passed controls have no errors
			and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		--COUNT: 16,462	