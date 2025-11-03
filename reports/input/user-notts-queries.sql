-- **DISCREPS** --
-- result changed
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
      and observations.final_cls = observations.dxai_cls
      and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%') -- the 'valid lims' should be from CSV as may be different for Viracor for example.
      and wells.resolution_codes like '%bla%'; -- bla is universal for all dbs (user changed classification code)
      
--ignored error
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
      and observations.final_cls = observations.machine_cls
      and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%')
		and wells.resolution_codes like '%bla%';

-- sample repeated
select * from wells, observations where
      observations.well_id = wells.id
      and observations.machine_cls != observations.dxai_cls
	AND  (wells.lims_status not like '%detected%' and wells.lims_status not like '%1500%' or wells.lims_status is null)
	and wells.error_code_id not in ('9a404521-fba9-48e4-bbf2-82e80204952a', '9a404521-48a5-433b-afc7-af042c747de7', '9d33a391-43e9-4b2f-ab23-0b57d08f3ef9')
	and wells.role_alias like 'Patient' and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		
	
-- **SOP**
-- sop repeat
	select * from wells where
		wells.role_alias = 'Patient'
		and wells.resolution_codes not like '%bla%' -- must have non bla (curve classification change) resolution code
		and ((wells.lims_status not like '%detected%' and wells.lims_status not like '%1500%') or wells.lims_status is null) -- no valid detected result or no lims at all (no lims = it has error code)
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		and (wells.error_code_id not in ('9a404521-fba9-48e4-bbf2-82e80204952a', '9a404521-48a5-433b-afc7-af042c747de7', '9a404521-8a32-425f-b53b-785b793b1ddb', '9d33a391-43e9-4b2f-ab23-0b57d08f3ef9') --not in control related issue, and not a classification error (WDCLS). Should use ec join not uuids.
			or wells.error_code_id is null)
		
-- sop unresolved
	select * from wells where
		wells.role_alias = 'Patient'
		and ((wells.resolution_codes not like '%bla%') or wells.resolution_codes is null) -- must have non bla (curve classification change) resolution code
		and wells.lims_status is null
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		and wells.error_code_id not in ('9a404521-fba9-48e4-bbf2-82e80204952a', '9a404521-48a5-433b-afc7-af042c747de7', '9a404521-8a32-425f-b53b-785b793b1ddb', '9d33a391-43e9-4b2f-ab23-0b57d08f3ef9' )

-- sop ignored
		select * from wells where
		wells.role_alias = 'Patient'
		and wells.resolution_codes not like '%bla%' -- must have non bla (curve classification change) resolution code, not bla (and not null)
		and (wells.lims_status like '%detected%' OR wells.lims_status like '%1500%')
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		
-- **VALID RESULTS (for summary)**
	--control repeat
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is not null
		and (wells.lims_status is not null or wells.error_code_id is not null) -- set all associated patient results to this repeat result, or has error
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		
	--controls error ignored
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is not null -- has a resolution
		and wells.lims_status is null -- no repeats etc
		and wells.error_code_id is null -- no error code
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		
	-- control error not resolved
	select * from wells where
		wells.role_alias != 'Patient' -- non patient
		and wells.resolution_codes is null -- no resolution (excludes above two categories)
		and (wells.lims_status is not null or wells.error_code_id is not null) -- well retains error or repeat status
		and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
		
		
-- patients with no warnings or issues
		select * from wells WHERE 
			wells.role_alias = 'Patient'
			and wells.resolution_codes is NULL 
			and (wells.lims_status like '%detected%' or wells.lims_status like '%1500%')
			and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
			
-- controls with no warnings or issues
		select * from wells WHERE 
			wells.role_alias != 'Patient'
			and wells.resolution_codes is NULL 
			and wells.lims_status is null -- passed controls have blank lims
			and wells.error_code_id is NULL  -- passed controls have no errors
			and wells.created_at > '2024-05-31' and wells.created_at < '2025-06-01'
			
