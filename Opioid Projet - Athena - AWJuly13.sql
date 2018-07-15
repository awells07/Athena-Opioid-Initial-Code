use ida_data;

/* Alex's code
from dsz_dwf_3nf.dsz_document_3nf a 
    join dsz_dwf_3nf.dsz_clinicalencounter_3nf b 
        on a.clinicalencounterid = b.clinicalencounterid
            and a.contextid = b.contextid 
    join dsz_dwf_3nf.dsz_provider_3nf c 
        on b.providerid = c.providerid
            and b.contextid = c.contextid
    join dsz_dwf_3nf.dsz_patientmedication_3nf d 
        on a.documentid = d.documentid
            and a.contextid = d.contextid
where a.patientid in (1342283, 1342284)
    and a.contextid = 7654 


[‎7/‎5/‎2018 4:03 PM]  Dalrymple, Gregory:  
select *
from dsz_athenadwf_3nf.dsz_appointment_3nf ap
join dsz_athenadwf_3nf.dsz_patientmedication_3nf pm on
        ap.contextid = pm.contextid
    and ap.patientid = pm.patientid
join dsz_athenadwf_3nf.dsz_claim_3nf cl on
        pm.contextid = cl.contextid
    and pm.patientid = cl.patientid
    and ap.appointmentid = cl.claimappointmentid
join dsz_athenadwf_3nf.dsz_document_3nf d on
        pm.patientid = d.patientid
    and pm.contextid = d.contextid
    and pm.chartid = d.chartid
where ap.appointmentdate between '2018-03-01 00:00:00' and current_timestamp()
and ap.contextid in (7598)
and ap.claimid in (cl.claimid, cl.originalclaimid)
and pm.chartid is not null; 
*/

use ida_data;

/* check out tables */
select top 100 * from athena.documentappointmentrequest;
select distinct medicationtype from athena.PatientMedication order by 1;
/* end */

/* Step a - create base fill table */
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_mems_rx', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_mems_rx;
select distinct a.context_id, ch.EnterpriseID, concat(a.context_id,' - ', ch.EnterpriseID) as unique_patient, doc.patientid, 
                a.documentid, ce.EncounterDate, a.medicationtype, /*a.MedicationID, a.filldate, pharmacyname,*/ med.ndc, doc.FBDMedID, 
				/*med.HIC1Description,*/ prescriptionfillquantity, numberofrefillsprescribed, doc.ChartID, ce.ClinicalEncounterID				
into #wells_june29_athena_mems_rx
from (select * 
      from athena.PatientMedication
	  where DeletedDatetime is null and
	        medicationtype in ('CLINICALPRESCRIPTION') and
	        context_id in ('7598', '8363') /* ININD, TXAUS*/ and
			PrescriptionFillQuantity is not null) a
inner join
     athena.document doc
on a.context_id=doc.context_id and
   a.documentid=doc.documentid
inner join
     (select * from athena.clinicalencounter where year(EncounterDate)>2016) ce
on doc.context_id=ce.context_id and
   doc.ClinicalEncounterID=ce.ClinicalEncounterID
inner join
     athena.Chart ch
on doc.context_id=ch.context_id and
   doc.ChartID=ch.ChartID
inner join
     athena.Medication med
on doc.context_id=med.context_id and
   doc.FBDMedID=med.FDBMedID
where upper(med.HIC3Description) like ('%NARCOTICS%') or
	  upper(med.MedicationName) like ('%AVINZA%') or 
	  upper(med.MedicationName) like ('%BUTRANS%') or 
	  upper(med.MedicationName) like ('%DOLOPHINE%') or 
	  upper(med.MedicationName) like ('%DURAGESIC%') or 
	  upper(med.MedicationName) like ('%EMBEDA%') or 
	  upper(med.MedicationName) like ('%EXALGO%') or 
	  upper(med.MedicationName) like ('%KADIAN%') or 
	  upper(med.MedicationName) like ('%MS CONTIN%') or 
	  upper(med.MedicationName) like ('%OPANA ER%') or 
	  upper(med.MedicationName) like ('%ORAMORPH%') or 
	  upper(med.MedicationName) like ('%TRAMADOL%') or
	  upper(med.MedicationName) like ('%OXYCONTIN%')	  	   
order by a.CONTEXT_ID, ch.EnterpriseID, ce.EncounterDate;
	create index athena_meds_mems_idxP on #wells_june29_athena_mems_rx(patientid);
	create index athena_meds_mems_idxC on #wells_june29_athena_mems_rx(context_id);
	create index athena_meds_mems_idxU on #wells_june29_athena_mems_rx(unique_patient);
	create index athena_meds_mems_idxE on #wells_june29_athena_mems_rx(EnterpriseID);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_mems_rx;
    select top 100 * from #wells_june29_athena_mems_rx;

/* Step b - join in appointmentid claimid 
IF OBJECT_ID('tempdb.dbo.#temp_to_drop', 'U') IS NOT NULL DROP TABLE #temp_to_drop;
select ap.context_id, ap.patientid, ap.appointmentid, ap.AppointmentDate, cl.claimid, cl.renderingproviderid
into #temp_to_drop
from athena.appointment ap
inner join
     athena.claim cl
on ap.context_id = cl.context_id and
   ap.patientid = cl.patientid and
   ap.appointmentid = cl.claimappointmentid
inner join
     (select distinct context_id, patientid, DocumentID from #wells_june29_athena_mems_rxb) rx
on ap.context_id = rx.context_id and
   ap.patientid = rx.patientid
where ap.claimid in (cl.claimid, cl.originalclaimid) and
      year(ap.AppointmentDate)>2017;
	create index athena_appt_mems_idxP on #temp_to_drop(patientid);
	create index athena_appt_mems_idxC on #temp_to_drop(context_id);

 Step c - join rx and appt/clm tables 
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_mems_rx', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_mems_rx;
select rx.*, temp.appointmentid, temp.AppointmentDate, temp.claimid
into #wells_june29_athena_mems_rx
from #wells_june29_athena_mems_rxb rx
left join
     #temp_to_drop temp
on rx.context_id=temp.context_id and
   rx.PatientID=temp.PatientID and
   rx.EncounterDate=temp.AppointmentDate; 
    create index athena_mems2_idxP on #wells_june29_athena_mems_rx(patientid);
	create index athena_mems2_idxC on #wells_june29_athena_mems_rx(context_id);
	create index athena_mems2_idxU on #wells_june29_athena_mems_rx(unique_patient);
			select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_mems_rx;
				select old_freq_temp1, new_freq_temp2, old_freq_temp1-new_freq_temp2 as freq_delta,
				       old_distinct_mems_temp1, new_distinct_mems_temp2, old_distinct_mems_temp1-new_distinct_mems_temp2 as mem_delta,
					   old_freq_temp1/old_distinct_mems_temp1 as old_clm_mem_ratio, new_freq_temp2/new_distinct_mems_temp2 as new_clm_mem_ratio
				from (select count(*) as old_freq_temp1, count(distinct unique_patient) as old_distinct_mems_temp1 from #wells_june29_athena_mems_rxb) a,
				     (select count(*) as new_freq_temp2, count(distinct unique_patient) as new_distinct_mems_temp2 from #wells_june29_athena_mems_rx) b;
    select top 100 * from #wells_june29_athena_mems_rx;
	
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_mems_rxb', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_mems_rxb;
IF OBJECT_ID('tempdb.dbo.#temp_to_drop', 'U') IS NOT NULL DROP TABLE #temp_to_drop; */
/* end Step 1 */

/* Step 2: Create unique patient demog table */
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_distmems', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_distmems;
select distinct a.*, concat(fn2,'|',ln2,'|',dob2) as fname_lname_dob /* exclude patients where this is null */
into #wells_june29_athena_distmems
from
(
select distinct a.CONTEXT_ID, a.EnterpriseID, b.unique_patient, a.FirstName, 
                a.LastName, a.Sex as gender, 
         UPPER(LTRIM(RTRIM(Replace(Replace(a.LastName,' ',''),'-','')))) as ln2,
	     UPPER(LTRIM(RTRIM(Replace(Replace(a.FirstName,' ',''),'-','')))) as fn2,
	     convert(varchar,dob,101) as dob2, floor(datediff(day,a.DOB,'2018-12-31')/365.25) as age, /* exclude patients where dob is null */
		    case when floor(datediff(day,a.DOB,'2018-12-31')/365.25)<18 then 'age <18'
			     when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 18 and 29 then 'age 18 to 29'
				 when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 30 and 39 then 'age 30 to 39'
				 when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 40 and 49 then 'age 40 to 49'
				 when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 50 and 59 then 'age 50 to 59'
			     when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 60 and 69 then 'age 60 to 69'
				 when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 70 and 75 then 'age 70 to 79'
				 when floor(datediff(day,a.DOB,'2018-12-31')/365.25) between 76 and 80 then 'age 80 to 85'
					else 'age >85' end as age_cohort,
	   isnull(a.Ethnicity,'NA') as ethnicity, isnull(a.Race,'NA') as race, isnull(a.MaritalStatus,'NA') as maritalstatus, 
	   case when a.DeceasedDate is null then '2099-12-31' else concat(datepart(year,a.DeceasedDate),'-',datepart(month,a.DeceasedDate),'-',datepart(day,a.DeceasedDate)) end as DeceasedDate,
	   a.City, a.[state], concat(a.city,' - ',a.[state]) as city_state, isnull(substring(a.Zip,1,5),'99999') as zip, /* exclude patients where city, state is null */
	   case when EmergencyContactRelationship is null then 'No EContact' else 'EContact Person' end as emergency_contact_flag
from athena.patient a 
inner join
    (select distinct context_id, enterpriseID, unique_patient from #wells_june29_athena_mems_rx) b
on a.context_id=b.context_id and
   a.EnterpriseID=b.EnterpriseID
) a;
   	--create index athena_fills_mems2_idxP on #wells_june29_athena_distmems(patientid);
	create index athena_fills_mems2_idxC on #wells_june29_athena_distmems(context_id);
	create index athena_fills_mems_idxU on #wells_june29_athena_distmems(unique_patient);
	create index athena_fills_mems_idxLN on #wells_june29_athena_distmems(fname_lname_dob);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_distmems;
    select top 100 * from #wells_june29_athena_distmems;
/* end step 2 */

/* Step 3: Create surgery table */
select top 100 * from athena.patientsurgery;

IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_patsurg', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_patsurg;
select distinct unique_patient, count(surgery_procedure) as surg_procs_all, count(distinct surgery_procedure) as surg_procs_distinct,
                case when count(surgery_procedure)=0 then 'no surgery' 
				     when count(surgery_procedure)>count(distinct surgery_procedure) then 'same proc >1' 
						else 'unique' end as surgery_uniqueness_ratio
into #wells_june29_athena_patsurg
from
(
select ch.context_id, ch.EnterpriseID, concat(ch.context_id,' - ', ch.EnterpriseID) as unique_patient, [Procedure] as surgery_procedure, 
       surgerydatetime, note as surgey_note
from athena.patientsurgery ps
inner join
     athena.Chart ch
on ps.context_id=ch.context_id and
   ps.ChartID=ch.ChartID
where year(surgerydatetime) between 2015 and 2018 /* last 3 to 4 years of surgery history */
) a
group by unique_patient;
   	--create index athena_fills_mems2_idxP on #wells_june29_athena_patsurg(patientid);
	--create index athena_fills_mems2_idxC on #wells_june29_athena_patsurg(context_id);
	create index athena_fills_mems_idxU on #wells_june29_athena_patsurg(unique_patient);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_patsurg;
    select top 100 * from #wells_june29_athena_patsurg;
/* end step 3 */

/* Step 4: Pull in all patient recs for the same time period
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_claim', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_claim;
select a.context_id, a.patientid, concat(a.context_id,' - ', a.patientid) as unique_patient, a.claimid, a.renderingproviderid, 
       a.claimappointmentid, a.servicedepartmentid, ChargeFromDate
into #wells_june29_athena_claim
from athena.claim a  /* use claim table to pull in patientid */
inner join
     athena.transactions b  /* use transactions table to restrict time frame */
on a.CONTEXT_ID = b.CONTEXT_ID and
   a.claimid = b.claimid
where year(b.ChargeFromDate)>2016;
   	create index athena_clm_mems2_idxP on #wells_june29_athena_claim(patientid);
	create index athena_clm_mems2_idxC on #wells_june29_athena_claim(context_id);
	create index athena_clm_mems_idxU on #wells_june29_athena_claim(unique_patient);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_claim;
    select top 100 * from #wells_june29_athena_claim;
 end step 4 */

/* Step 5: Identify patient dx */ /* see if need to aggregate to ClinicalEncounterID level b/c multiple dx per id */
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_clmdx', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_clmdx;
select distinct unique_patient, ClinicalEncounterID, 
                --count(distinct DiagnosisCode) as diagnosiscode_distinct_total, 
				
				/* gross totals */
                count(DiagnosisCode) as diagnosiscode_all_total,		
                sum(case when oncology_flag='oncology' then 1 else 0 end) as oncology_visits,
				sum(case when pain_type='No Pain' then 0 else 1 end) as pain_visits,
				sum(case when depression_flag='No Depression' then 0 else 1 end) as depression_visits,

				/* specific totals */
				sum(case when depression_flag='Major depressive disorder, single episode' then 1 else 0 end) as depression_single_episode_visits,
				sum(case when depression_flag='Major depressive disorder, recurrent' then 1 else 0 end) as depression_recurrent_visits,
				sum(case when pain_type='pain, unspecified' then 1 else 0 end) as unspecified_pain_visits,
				sum(case when pain_type='acute and chronic pain, not elsewhere classified' then 1 else 0 end) as nec_pain_visits,
				sum(case when pain_type='abdomen pain' then 1 else 0 end) as abdomen_pain_visits,
				sum(case when pain_type='back pain' then 1 else 0 end) as back_pain_visits,
				sum(case when pain_type='kidney stones' then 1 else 0 end) as kidney_stones_visits,
				sum(case when pain_type='ankle/foot injuries' then 1 else 0 end) as ankle_foot_visits,				
				sum(case when pain_type='breast pain' then 1 else 0 end) as breast_pain_visits,
				sum(case when pain_type='chest pain' then 1 else 0 end) as chest_pain_visits,
				sum(case when pain_type='ear pain' then 1 else 0 end) as ear_pain_visits,
				sum(case when pain_type='eye pain' then 1 else 0 end) as eye_pain_visits,
				sum(case when pain_type='headache' then 1 else 0 end) as headache_visits,
				sum(case when pain_type='joint pain' then 1 else 0 end) as joint_pain_visits,
				sum(case when pain_type='limb pain' then 1 else 0 end) as limb_pain_visits,
				--sum(case when pain_type='lumbar region pain' then 1 else 0 end) as lumbar_pain_visits,
				sum(case when pain_type='pelvic and perineal pain' then 1 else 0 end) as pelvic_perineal_visits,
				sum(case when pain_type='shoulder pain' then 1 else 0 end) as shoulder_pain_visits,
				sum(case when pain_type='spine pain' then 1 else 0 end) as spine_pain_visits,
				sum(case when pain_type='throat pain' then 1 else 0 end) as throat_pain_visits,
				sum(case when pain_type='tongue pain' then 1 else 0 end) as tongue_pain_visits,
				sum(case when pain_type='tooth pain' then 1 else 0 end) as tooth_pain_visits,
				sum(case when pain_type='renal colic' then 1 else 0 end) as renal_colic_visits,
				sum(case when pain_type='pain disorders exclusively related to psychological factors' then 1 else 0 end) as psych_pain_visits,

				/* disease counts */
				sum(case when disease_flag='diabetes' then 1 else 0 end) as diabetes_visits, 
				sum(case when disease_flag='hf' then 1 else 0 end) as hf_visits,
				sum(case when disease_flag='ischemic hd' then 1 else 0 end) as ischemic_hd_visits,
				sum(case when disease_flag='kidney disease' then 1 else 0 end) as kidney_disease_visits,
				sum(case when disease_flag='osteoporosis' then 1 else 0 end) as osteoporosis_visits,
				sum(case when disease_flag='copd' then 1 else 0 end) as copd_visits,
				sum(case when disease_flag='stroke' then 1 else 0 end) as stroke_visits
into #wells_june29_athena_clmdx
from 
(
select distinct rx.context_id, rx.EnterpriseID, rx.unique_patient, lu.diagnosiscode, rx.ClinicalEncounterID,
       case when lu.DiagnosisCode in ('Z12.11','R11.0','R11.2','R53.0','R53.1','R53.81','R53.83','G93.3','E86.0','D61.818','D61.9','D64.0','D64.1','D64.2','D64.3','D64.81',
	                                 'C82.50', 'C82.59', 'C84.90', 'C84.99', 'C84.A0', 'C84.A9', 'C84.Z0', 'C84.Z9', 'C85.10', 'C85.19', 'C85.20', 'C85.29', 'C85.80', 'C85.89', 
									 'C85.90', 'C85.99', 'C86.4', 'C78.00', 'C78.01', 'C78.02', 'D03.4', 'C50.911', 'C50.912', 'C50.919', 'C55', 'C56.1', 'C56.2', 'C56.9',
                                     'C18.9','C43.4', 'C44.01', 'C4A.0', 'C4A.10', 'C4A.11', 'C4A.12', 'C4A.20', 'C4A.21', 'C4A.22', 'C4A.30', 'C4A.31', 'C4A.39', 'C73', 
									 'C74.00', 'C74.01', 'C74.02', 'C74.10', 'C74.11', 'C74.12', 'C74.90', 'C74.91', 'C74.92') then 'oncology' else 'no oncology' end as oncology_flag,
									 /* https://icdcodelookup.com/icd-10/common-codes/oncology */

		case when substring(lu.DiagnosisCode,1,3) in ('R52') then 'pain, unspecified'
		     when substring(lu.DiagnosisCode,1,3) in ('G89') then 'acute and chronic pain, not elsewhere classified'
             when substring(lu.DiagnosisCode,1,3) in ('R10') then 'abdomen pain'
             when substring(lu.DiagnosisCode,1,4) in ('M549', 'M545') then 'back pain'
			 when substring(lu.DiagnosisCode,1,4) in ('N200') then 'kidney stones'
			 when substring(lu.DiagnosisCode,1,3) between 'S90' and 'S99' then 'ankle/foot injuries' /* https://icdcodelookup.com/icd-10/common-codes/podiatry */
             when substring(lu.DiagnosisCode,1,4) in ('N644') then 'breast pain'
             when substring(lu.DiagnosisCode,1,4) between ('R071') and ('R079') then 'chest pain'
             when substring(lu.DiagnosisCode,1,4) in ('H920') then 'ear pain'
             when substring(lu.DiagnosisCode,1,4) in ('H571') then 'eye pain'
             when substring(lu.DiagnosisCode,1,3) in ('R51') then 'headache'
             when substring(lu.DiagnosisCode,1,4) in ('M255') then 'joint pain'
             when substring(lu.DiagnosisCode,1,4) in ('M796') then 'limb pain'
             --when substring(lu.DiagnosisCode,1,4) in ('M545') then 'lumbar region pain'
             when substring(lu.DiagnosisCode,1,4) in ('R102') then 'pelvic and perineal pain'
             when substring(lu.DiagnosisCode,1,5) in ('M2551') then 'shoulder pain'
             when substring(lu.DiagnosisCode,1,3) in ('M54') then 'spine pain'
             when substring(lu.DiagnosisCode,1,4) in ('R070') then 'throat pain'
             when substring(lu.DiagnosisCode,1,4) in ('K146') then 'tongue pain'
             when substring(lu.DiagnosisCode,1,4) in ('K088') then 'tooth pain'
             when substring(lu.DiagnosisCode,1,3) in ('N23') then 'renal colic'
             when substring(lu.DiagnosisCode,1,5) in ('F4541') then 'pain disorders exclusively related to psychological factors'
				else 'No Pain' end as pain_type, /* https://www.icd10data.com/ICD10CM/Codes/G00-G99/G89-G99/G89-/G89.4 */

		case when substring(lu.DiagnosisCode,1,3) in ('F32') then 'Major depressive disorder, single episode'
		     when substring(lu.DiagnosisCode,1,3) in ('F33') then 'Major depressive disorder, recurrent'
			 	else 'No Depression' end as depression_flag, /* http://icd10cmcode.com/what-is-the-icd-10-for-depression.php */

		case when substring(lu.DiagnosisCode,1,4) in ('Z794') or substring(lu.DiagnosisCode,1,3) in ('E08','E09','E10','E11','E13') then 'diabetes'
		     when substring(lu.DiagnosisCode,1,5) in ('39891', '40201', '40211', '40291', '40401', '40403', '40411', '40413', '40491', '40493', 
			                                          '42821', '42822', '42823', '42831', '42832', '42833', '42841', '42842', '42843', 'I0981',
												      'I5020', 'I5021', 'I5022', 'I5023', 'I5030', 'I5031', 'I5032', 'I5033', 'I5040', 'I5041',
												      'I5042', 'I5043') or
			      substring(lu.DiagnosisCode,1,4) in ('4281', '4282', '4283', '4284', '4289', 'I110', 'I130', 'I132', 'I501', 'I509') then 'hf'
             when substring(lu.DiagnosisCode,1,3) in ('I25') then 'ischemic hd'
			 when substring(lu.DiagnosisCode,1,3) in ('N17','N18','N19') then 'kidney disease'
			 when substring(lu.DiagnosisCode,1,3) in ('M80','M81','M82') then 'osteoporosis'
			 when substring(lu.DiagnosisCode,1,3) in ('J44') then 'copd'
			 when substring(lu.DiagnosisCode,1,3) in ('I64') or substring(lu.DiagnosisCode,1,4) in ('I694','I619','I639') then 'stroke' 
				else 'not a core disease' end as disease_flag
from #wells_june29_athena_mems_rx rx
inner join
    athena.ClinicalEncounterDiagnosis dx
on rx.context_id=dx.context_id and
   rx.ClinicalEncounterID=dx.ClinicalEncounterID
inner join
     athena.ClinicalEncounterdxicd10 icd
on dx.ClinicalEncounterDXID=icd.ClinicalEncounterDXID
inner join
     (select * from athena.ICDCodeAll where DiagnosisCodeSet='ICD10') lu
on icd.IcdCodeID=lu.IcdCodeID
) a
group by unique_patient, ClinicalEncounterID;
	--create index athena_dx_mems_idxP on #wells_june29_athena_clmdx(EnterpriseID);
	--create index athena_dx_mems_idxC on #wells_june29_athena_clmdx(context_id);
	create index athena_dx_mems_idxU on #wells_june29_athena_clmdx(unique_patient);
	create index athena_dx_mems_idxCEID on #wells_june29_athena_clmdx(ClinicalEncounterID);
			select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_clmdx;
    select top 100 * from #wells_june29_athena_clmdx;

		select *
		from #wells_june29_athena_clmdx
		where unique_patient in (select distinct unique_patient 
		                         from (select distinct unique_patient, ClinicalEncounterID, count(*) as freq
								       from #wells_june29_athena_clmdx
									   group by unique_patient, ClinicalEncounterID
									   having count(*)>1) a)
		order by 3, 5; 
								 
/* end step 5 */


/* Step 6: Join tables created above */
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_temp1', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_temp1;
select mems.*, rx.documentid, rx.EncounterDate, rx.medicationtype, rx.ndc, rx.FBDMedID, isnull(rx.prescriptionfillquantity,0) as prescriptionfillquantity, 
       isnull(rx.numberofrefillsprescribed,0) as numberofrefillsprescribed, rx.ChartID, rx.ClinicalEncounterID, 
	   isnull(surg.surg_procs_all,0) as surg_procs_all, isnull(surg.surg_procs_distinct,0) as surg_procs_distinct, 
	   case when surg.surgery_uniqueness_ratio is null then 'no surgery' else surg.surgery_uniqueness_ratio end as surgery_uniqueness_ratio, 
	   isnull(diagnosiscode_all_total,0) as diagnosiscode_all_total, isnull(oncology_visits,0) as oncology_visits, 
	   isnull(pain_visits,0) as pain_visits, isnull(depression_visits,0) as depression_visits,
	   isnull(depression_single_episode_visits,0) as depression_single_episode_visits, 
	   isnull(depression_recurrent_visits,0) as depression_recurrent_visits, isnull(unspecified_pain_visits,0) as unspecified_pain_visits, 
	   isnull(nec_pain_visits,0) as nec_pain_visits, isnull(abdomen_pain_visits,0) as abdomen_pain_visits, isnull(back_pain_visits,0) as back_pain_visits, 
	   isnull(breast_pain_visits,0) as breast_pain_visits, isnull(chest_pain_visits,0) as chest_pain_visits, isnull(ear_pain_visits,0) as ear_pain_visits, 
	   isnull(eye_pain_visits,0) as eye_pain_visits, isnull(headache_visits,0) as headache_visits, isnull(joint_pain_visits,0) as joint_pain_visits, 
	   isnull(limb_pain_visits,0) as limb_pain_visits, /*lumbar_pain_visits,*/ isnull(pelvic_perineal_visits,0) as pelvic_perineal_visits, 
	   isnull(kidney_stones_visits,0) as kidney_stone_pain_visits,isnull(ankle_foot_visits,0) as ankle_foot_pain_visits,
	   isnull(shoulder_pain_visits,0) as shoulder_pain_visits, isnull(spine_pain_visits,0) as spine_pain_visits, 
	   isnull(throat_pain_visits,0) as throat_pain_visits, isnull(tongue_pain_visits,0) as tongue_pain_visits, 
	   isnull(tooth_pain_visits,0) as tooth_pain_visits, isnull(renal_colic_visits,0) as renal_colic_visits, 
	   isnull(psych_pain_visits,0) as psych_pain_visits,
	   isnull(diabetes_visits,0) as diabetes_visits, 
	   isnull(hf_visits,0) as hf_visits,
	   isnull(ischemic_hd_visits,0) as ischemic_hd_visits,
	   isnull(kidney_disease_visits,0) as kidney_disease_visits,
	   isnull(osteoporosis_visits,0) as osteoporosis_visits,
	   isnull(copd_visits,0) as copd_visits,
	   isnull(stroke_visits,0) as stroke_visits 
into #wells_june29_athena_temp1
from #wells_june29_athena_distmems mems
inner join
     #wells_june29_athena_mems_rx rx  /* inner join as all mems must have >=1 rx */
on mems.unique_patient = rx.unique_patient 
left join
     #wells_june29_athena_patsurg surg  /* left join as not all patients have had surgery */
on mems.unique_patient=surg.unique_patient
left join
     #wells_june29_athena_clmdx dx  /* left join as not all patients will have an encounter (phone fill, e-fill) */
on mems.unique_patient=dx.unique_patient and
   rx.ClinicalEncounterID=dx.ClinicalEncounterID
order by unique_patient, EncounterDate;
   	--create index athena_temp1_idxP on #wells_june29_athena_temp1(patientid);
	create index athena_temp1_idxC on #wells_june29_athena_temp1(context_id);
	create index athena_temp1_idxU on #wells_june29_athena_temp1(unique_patient);
			select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_temp1;
    select top 100 * from #wells_june29_athena_temp1;
		/* check table change */
		select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
		from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from #wells_june29_athena_temp1) a,
		     (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #wells_june29_athena_mems_rx) b;
	   
/* put into permanent table */
		drop table [DSDWDev].aw.AW_athena_opioids_memdetail_july3;
	select *
	into [DSDWDev].aw.AW_athena_opioids_memdetail_july3
	from #wells_june29_athena_temp1;
		    --create index athena_mems_idxP on [DSDWDev].aw.AW_athena_opioids_memdetail_july3(patientid);
			create index athena_mems_idxC on [DSDWDev].aw.AW_athena_opioids_memdetail_july3(context_id);
			create index athena_mems_idxU on [DSDWDev].aw.AW_athena_opioids_memdetail_july3(unique_patient);
			--create index athena_mems_idxCLM on [DSDWDev].aw.AW_athena_opioids_memdetail_july3(claimid);

			select top 100 * from [DSDWDev].aw.AW_athena_opioids_memdetail_july3;

			/* check copied table */
			select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
			from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from [DSDWDev].aw.AW_athena_opioids_memdetail_july3) a,
				 (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #wells_june29_athena_temp1) b;
/* end step 6 */


/* Step 7: create unique patient table w/ total meds and min fill date */
use DSDWDev;

--select top 100 * from [DSDWDev].aw.AW_athena_opioids_memdetail_july3;

IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_temp2', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_temp2;
select distinct unique_patient,                
                sum(prescriptionfillquantity) as total_opioid_prescribed,
				min(EncounterDate) as min_filldate,
				sum(oncology_visits) as oncology_visits_total,
				sum(pain_visits) as pain_visits_total, 
				sum(depression_visits) as depression_visits_total,
				sum(diagnosiscode_all_total) as diagnosiscode_all_total
into #wells_june29_athena_temp2
from [DSDWDev].aw.AW_athena_opioids_memdetail_july3
group by unique_patient;
	--create index athena_mems_idxP on [DSDWDev].aw.#wells_june29_athena_temp2(patientid);
	--create index athena_mems_idxC on [DSDWDev].aw.#wells_june29_athena_temp2(context_id);
	create index athena_mems_idxU on #wells_june29_athena_temp2(unique_patient);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num from #wells_june29_athena_temp2;
/* end step 7 */

/* Step 8: Create strata  */
	/* Step 8a: compute median for non-zero features */
	SELECT MIN(oncology_visits_total) 
	FROM (SELECT TOP 50 PERCENT oncology_visits_total 
		  FROM #wells_june29_athena_temp2 
		  WHERE oncology_visits_total>0 
		  ORDER BY oncology_visits_total DESC) a; --1

	SELECT MIN(pain_visits_total) 
	FROM (SELECT TOP 50 PERCENT pain_visits_total 
		  FROM #wells_june29_athena_temp2 
		  WHERE pain_visits_total>0
		  ORDER BY pain_visits_total DESC) a; --2

	SELECT MIN(depression_visits_total) 
	FROM (SELECT TOP 50 PERCENT depression_visits_total 
		  FROM #wells_june29_athena_temp2 
		  WHERE depression_visits_total>0 
		  ORDER BY depression_visits_total DESC) a; --1

	SELECT MIN(diagnosiscode_all_total) 
	FROM (SELECT TOP 50 PERCENT diagnosiscode_all_total 
		  FROM #wells_june29_athena_temp2 
		  WHERE diagnosiscode_all_total>0 
		  ORDER BY diagnosiscode_all_total DESC) a; --5
	/* end Step 8a */

/* Step 8b */
IF OBJECT_ID('tempdb.dbo.#wells_june29_athena_strata', 'U') IS NOT NULL DROP TABLE #wells_june29_athena_strata;
select distinct unique_patient,
concat(age_cohort, ' | ', gender, ' | ',  Race, ' | ',  /*MaritalStatus, ' | ',  emergency_contact_flag, ' | ', context_id, ' | ',*/
              (case when diabetes_visits+hf_visits+ischemic_hd_visits+kidney_disease_visits+osteoporosis_visits+copd_visits+stroke_visits=0 then 'no disease' 
			        when diabetes_visits+hf_visits+ischemic_hd_visits+kidney_disease_visits+osteoporosis_visits+copd_visits+stroke_visits=1 then '1 chronic' 
					when diabetes_visits+hf_visits+ischemic_hd_visits+kidney_disease_visits+osteoporosis_visits+copd_visits+stroke_visits>1 then '2+ chronics' end), ' | ',
			  /*(case when diabetes_visits=0 or diabetes_visits is null then 'not diabetic' when diabetes_visits>1 then 'diabetic' end), ' | ', /* 2+ hits to be diseased */
			  (case when hf_visits=0 or hf_visits is null then 'not hf' when hf_visits>1 then 'hf' end), ' | ',
			  (case when ischemic_hd_visits=0 or ischemic_hd_visits is null then 'not ischemic hd' when ischemic_hd_visits>1 then 'ischemic hd' end), ' | ',
			  (case when kidney_disease_visits=0 or kidney_disease_visits is null then 'not kidney disease' when kidney_disease_visits>1 then 'kidney disease' end), ' | ',
			  (case when osteoporosis_visits=0 or osteoporosis_visits is null then 'not osteo' when osteoporosis_visits>1 then 'osteo' end), ' | ',
			  (case when copd_visits=0 or copd_visits is null then 'not copd' when copd_visits>1 then 'copd' end), ' | ',
			  (case when stroke_visits=0 or stroke_visits is null then 'not stroke' when stroke_visits>1 then 'stroke' end), ' | ',*/
	          (case when surg_procs_all=0 or surg_procs_all is null then 'No Surgery' else 'Surgery' end), ' | ',  
			  (case when diagnosiscode_all_total=0 or diagnosiscode_all_total is null then '0 dx' when diagnosiscode_all_total <=5 then 'med dx' else '>med dx' end), ' | ', 
			  (case when oncology_visits_total=0 or oncology_visits_total is null then '0 onc' when oncology_visits_total <2 then 'med onc' else '>med onc' end), ' | ', 
			  (case when pain_visits_total=0 or pain_visits_total is null then '0 pain' when pain_visits_total <=2 then 'med pain' else '>med pain' end), ' | ',
			  (case when depression_visits_total=0 or depression_visits_total is null then '0 depress' when depression_visits_total <2 then 'med depress' else '>med depress' end)
	   ) as micro_strata

			 /* (case when oncology_visits=0 or oncology_visits is null then '0 onc' when oncology_visits <=11 then 'med onc' else '>med onc' end), ' | ', 
	          (case when depression_single_episode_visits=0 or depression_single_episode_visits is null then '0 dep single' when depression_single_episode_visits <=10 then 'med dep single' else '>med dep single' end), ' | ',  
			  (case when depression_recurrent_visits=0 or depression_recurrent_visits is null then '0 dep rec' when depression_recurrent_visits <=11 then 'med dep rec' else '>med dep rec' end), ' | ',
			  (case when unspecified_pain_visits=0 or unspecified_pain_visits is null then '0 unspec pain' when unspecified_pain_visits <=12 then 'med unspec pain' else '>med unspec pain' end), ' | ',    
			  (case when back_pain_visits=0 or back_pain_visits is null then '0 back pain' when back_pain_visits <=9 then 'med back pain' else '>med back pain' end), ' | ',
			  (case when kidney_stone_pain_visits=0 or kidney_stone_pain_visits is null then '0 kidney stone pain' when kidney_stone_pain_visits <=11 then 'med kidney stone pain' else '>med kidney stone pain' end), ' | ',
			  (case when ankle_foot_pain_visits=0 or ankle_foot_pain_visits is null then '0 ankle pain' when ankle_foot_pain_visits <=7 then 'med ankle pain' else '>med ankle pain' end), ' | ',
			  (case when nec_pain_visits=0 or nec_pain_visits is null then '0 nec pain' when nec_pain_visits <=10 then 'med nec pain' else '>med nec pain' end), ' | ',
			  (case when abdomen_pain_visits=0 or abdomen_pain_visits is null then '0 abd pain' when abdomen_pain_visits <=9 then 'med abd pain' else '>med abd pain' end), ' | ', 
			  (case when headache_visits=0 or headache_visits is null then '0 headache' when headache_visits <=6 then 'med headache' else '>med headache' end), ' | ',
			  (case when spine_pain_visits=0 or spine_pain_visits is null then '0 spine pain' when spine_pain_visits <=10 then 'med spine pain' else '>med spine pain' end), ' | ', 
			  (case when breast_pain_visits=0 or breast_pain_visits is null then '0 breast pain' when breast_pain_visits <=6 then 'med breast pain' else '>med breast pain' end), ' | ',
			  (case when chest_pain_visits=0 or chest_pain_visits is null then '0 chest pain' when chest_pain_visits <=12 then 'med chest pain' else '>med chest pain' end), ' | ',
			  (case when ear_pain_visits=0 or ear_pain_visits is null then '0 ear pain' when ear_pain_visits <=6 then 'med ear pain' else '>med ear pain' end), ' | ',
			  (case when eye_pain_visits=0 or eye_pain_visits is null then '0 eye pain' when eye_pain_visits <=6 then 'med eye pain' else '>med eye pain' end), ' | ',
			  (case when joint_pain_visits=0 or joint_pain_visits is null then '0 joint pain' when joint_pain_visits <=9 then 'med joint pain' else '>med joint pain' end), ' | ',
			  (case when limb_pain_visits=0 or limb_pain_visits is null then '0 limb pain' when limb_pain_visits <=8 then 'med limb pain' else '>med limb pain' end), ' | ',
			  (case when throat_pain_visits=0 or throat_pain_visits is null then '0 throat pain' when throat_pain_visits <=9 then 'med throat pain' else '>med throat pain' end), ' | ',
			  (case when tongue_pain_visits=0 or tongue_pain_visits is null then '0 tongue pain' when tongue_pain_visits <=6 then 'med tongue pain' else '>med tongue pain' end), ' | ',
			  (case when tooth_pain_visits=0 or tooth_pain_visits is null then '0 tooth pain' when tooth_pain_visits <=5 then 'med tooth pain' else '>med tooth pain' end), ' | ',
			  (case when renal_colic_visits=0 or renal_colic_visits is null then '0 renal colic' when renal_colic_visits <=9 then 'med renal colic' else '>med renal colic' end)) as micro_strata*/
into #wells_june29_athena_strata
from
(
select distinct unique_patient, age_cohort, gender, Race, MaritalStatus, emergency_contact_flag, context_id,
                sum(diagnosiscode_all_total) as diagnosiscode_all_total,
				sum(surg_procs_all) as surg_procs_all,
                sum(oncology_visits) as oncology_visits_total,
				sum(pain_visits) as pain_visits_total, 
				sum(depression_visits) as depression_visits_total,
				sum(depression_single_episode_visits) as depression_single_episode_visits,
				sum(depression_recurrent_visits) as depression_recurrent_visits,
				sum(unspecified_pain_visits) as unspecified_pain_visits,
				sum(nec_pain_visits) as nec_pain_visits,
				sum(abdomen_pain_visits) as abdomen_pain_visits,
				sum(back_pain_visits) as back_pain_visits,
				sum(kidney_stone_pain_visits) as kidney_stone_pain_visits,
				sum(ankle_foot_pain_visits) as ankle_foot_pain_visits,				
				sum(breast_pain_visits) as breast_pain_visits,
				sum(chest_pain_visits) as chest_pain_visits,
				sum(ear_pain_visits) as ear_pain_visits,
				sum(eye_pain_visits) as eye_pain_visits,
				sum(headache_visits) as headache_visits,
				sum(joint_pain_visits) as joint_pain_visits,
				sum(limb_pain_visits) as limb_pain_visits,
				sum(pelvic_perineal_visits) as pelvic_perineal_visits,
				sum(shoulder_pain_visits) as shoulder_pain_visits,
				sum(spine_pain_visits) as spine_pain_visits,
				sum(throat_pain_visits) as throat_pain_visits,
				sum(tongue_pain_visits) as tongue_pain_visits,
				sum(tooth_pain_visits) as tooth_pain_visits,
				sum(renal_colic_visits) as renal_colic_visits,
				sum(psych_pain_visits) as psych_pain_visits,
			    sum(diabetes_visits) as diabetes_visits, 
			    sum(hf_visits) as hf_visits,
			    sum(ischemic_hd_visits) as ischemic_hd_visits,
			    sum(kidney_disease_visits) as kidney_disease_visits,
			    sum(osteoporosis_visits) as osteoporosis_visits,
			    sum(copd_visits) as copd_visits,
			    sum(stroke_visits) as stroke_visits 
from [DSDWDev].aw.AW_athena_opioids_memdetail_july3
group by unique_patient, age_cohort, gender, Race, MaritalStatus, emergency_contact_flag, context_id
) a;
	create index athena_mems_idxU on #wells_june29_athena_strata(unique_patient);
	create index athena_mems_idxS on #wells_june29_athena_strata(micro_strata);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num,
		       count(distinct micro_strata) as micro_strata_ct
		from #wells_june29_athena_strata;
	select top 100 * from #wells_june29_athena_strata;
/* end Step 8b */
/* end Step 8 */

/* Step 9: Create unique patient table with random id assigned */
IF OBJECT_ID('tempdb.dbo.#wells_athena_opioid_mems_distinct', 'U') IS NOT NULL DROP TABLE #wells_athena_opioid_mems_distinct;
select *
into #wells_athena_opioid_mems_distinct
from (select distinct unique_patient, newid() as randGUID
      from #wells_june29_athena_temp2) a
order by 2;
	create index athena_mems_idxU on #wells_athena_opioid_mems_distinct(unique_patient);
		select top 100 * from #wells_athena_opioid_mems_distinct;
/* end step 9 */

/* Step 10: Create cumulative meds field, by fill date */
IF OBJECT_ID('tempdb.dbo.#temp1', 'U') IS NOT NULL DROP TABLE #temp1;
select distinct unique_patient, encounterdate, ndc, prescriptionfillquantity,
	             concat(convert(varchar,encounterdate,102), ndc, prescriptionfillquantity) as tiebreaker
into #temp1
from [DSDWDev].aw.AW_athena_opioids_memdetail_july3
where year(EncounterDate)>2016
order by unique_patient, encounterdate;
		select * from #temp1 where unique_patient='7598 - 100551' order by EncounterDate; 
		select * from #temp1 where unique_patient='7598 - 1090615' order by EncounterDate;

IF OBJECT_ID('tempdb.dbo.#temp2', 'U') IS NOT NULL DROP TABLE #temp2;
select *, row_number() OVER(PARTITION BY unique_patient ORDER BY tiebreaker) AS rowid_encounter
into #temp2
from #temp1
order by encounterdate;
		select * from #temp2 where unique_patient='7598 - 100551' order by EncounterDate;
		select * from #temp2 where unique_patient='7598 - 1090615' order by EncounterDate;

IF OBJECT_ID('tempdb.dbo.#temp3', 'U') IS NOT NULL DROP TABLE #temp3;
select a.unique_patient, a.encounterdate, a.ndc, a.prescriptionfillquantity, b.randGUID, c.micro_strata, a.rowid_encounter
into #temp3
from #temp2 a
inner join
	 #wells_athena_opioid_mems_distinct b
on a.unique_patient = b.unique_patient
inner join
	 #wells_june29_athena_strata c
on b.unique_patient = c.unique_patient
order by a.unique_patient,  a.rowid_encounter;
		select * from #temp3 where unique_patient='7598 - 100551' order by EncounterDate;
		select * from #temp3 where unique_patient='7598 - 1090615' order by EncounterDate;
	
IF OBJECT_ID('tempdb.dbo.#wells_opioid_cumul', 'U') IS NOT NULL DROP TABLE #wells_opioid_cumul;
SELECT a.*, SUM(prescriptionfillquantity) OVER(PARTITION BY unique_patient ORDER BY rowid_encounter) AS cumul_prescritionfillqty
into #wells_opioid_cumul
FROM #temp3 a
ORDER BY rowid_encounter;
	select * from #wells_opioid_cumul where unique_patient='7598 - 100551' order by EncounterDate; /* multiple same day rx */
	select * from #wells_opioid_cumul where unique_patient='7598 - 1090615' order by EncounterDate;

	create index athena_mems_idxU on #wells_opioid_cumul(unique_patient);
	create index athena_mems_idxS on #wells_opioid_cumul(micro_strata);
		select count(*) as freq, count(distinct unique_patient) as mems from #wells_opioid_cumul;
/* end step 10 */

/* Step 11: Create thresholds of opioid rx fills by strata */
IF OBJECT_ID('tempdb.dbo.#wells_opioid_strata_percent', 'U') IS NOT NULL DROP TABLE #wells_opioid_strata_percent;
select unique_patient, micro_strata, 
       PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY max_cumul_rxfill) OVER (PARTITION BY micro_strata) AS perct80_prescritionfillqty
into #wells_opioid_strata_percent 
from (select distinct unique_patient, micro_strata, max(cumul_prescritionfillqty) as max_cumul_rxfill
      from #wells_opioid_cumul 
	  group by unique_patient, micro_strata) a
order by 2;
    create index athena_mems_idxU on #wells_opioid_strata_percent(unique_patient);
	create index athena_mems_idxS on #wells_opioid_strata_percent(micro_strata);
		select count(distinct micro_strata) as unique_strata, count(unique_patient) as mems from #wells_opioid_strata_percent;
			select top 1000 * from #wells_opioid_strata_percent;

/* join in the threshold by strata  */
IF OBJECT_ID('tempdb.dbo.#wells_opioid_cumul_with_strata_percent', 'U') IS NOT NULL DROP TABLE #wells_opioid_cumul_with_strata_percent;
select a.*, c.strata_mems, b.perct80_prescritionfillqty, 
       case when a.cumul_prescritionfillqty>=b.perct80_prescritionfillqty then 1 else 0 end as met_exceed_threshold
into #wells_opioid_cumul_with_strata_percent
from #wells_opioid_cumul a
inner join
     #wells_opioid_strata_percent b
on a.unique_patient=b.unique_patient
left join
     (select distinct micro_strata, count(*) as strata_mems
	  from #wells_opioid_strata_percent
	  group by micro_strata) c
on a.micro_strata=c.micro_strata
order by a.unique_patient, a.rowid_encounter;
	 create index athena_mems_idxU on #wells_opioid_cumul_with_strata_percent(unique_patient);
	 create index athena_mems_idxS on #wells_opioid_cumul_with_strata_percent(micro_strata);
		select count(*) as freq, count(distinct unique_patient) as distinct_mems, count(*) - count(distinct unique_patient) as error_num 
		from #wells_opioid_cumul_with_strata_percent;
			select top 1000 * from #wells_opioid_cumul_with_strata_percent;

				select * from #wells_opioid_cumul_with_strata_percent where unique_patient='7598 - 100551' order by EncounterDate; /* multiple same day rx */
				select * from #wells_opioid_cumul_with_strata_percent where unique_patient='7598 - 1090615' order by EncounterDate;
/* end step 11 */

/* Step 12: Compute time b/t fill dates  */
IF OBJECT_ID('tempdb.dbo.#temp1', 'U') IS NOT NULL DROP TABLE #temp1;
select *
into #temp1
from
(
select distinct b.unique_patient, a.EncounterDate as lag_filldate, b.EncounterDate as current_filldate, 
	            datediff(day,a.EncounterDate, b.EncounterDate) as filldatediff, a.rowid_encounter
from #wells_opioid_cumul_with_strata_percent a 
inner join
	  #wells_opioid_cumul_with_strata_percent b        
 on a.unique_patient=b.unique_patient and 
    a.rowid_encounter=b.rowid_encounter - 1
) a
order by 1, 5;
		select count(*) as freq, count(distinct unique_patient) as mems from #temp1;
			select * from #temp1 where unique_patient='7598 - 1090615' order by 1, 5;  /* '7598 - 1016594'     '7598 - 1090615'  */

			/* check new table */
			select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
			from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from #temp1) a,
				 (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #wells_opioid_cumul_with_strata_percent) b;
			/*
			new_ct	old_ct	ct_delta	new_mems	old_mems	mem_delta
			37000	57310	-20310			9219	20310		-11091
			*/

IF OBJECT_ID('tempdb.dbo.#temp2', 'U') IS NOT NULL DROP TABLE #temp2;
select *
into #temp2
from #wells_opioid_cumul_with_strata_percent
where concat(unique_patient,rowid_encounter) not in (select distinct identifier from (select concat(unique_patient,rowid_encounter) as identifier from #temp1) a)
order by unique_patient, rowid_encounter; /* (20310 rows affected) -- matches to row delta from #temp1 */
		select * from #temp2 order by unique_patient, encounterdate /* 7598 - 100551 */

IF OBJECT_ID('tempdb.dbo.#temp3', 'U') IS NOT NULL DROP TABLE #temp3;
select *, SUM(filldatediff) OVER(PARTITION BY unique_patient ORDER BY rowid_encounter) AS cumul_filldatediff
into #temp3
from
(
select unique_patient, EncounterDate as lag_filldate, EncounterDate as current_filldate, 0 as filldatediff, rowid_encounter
from #temp2
union all
select unique_patient, lag_filldate, current_filldate, filldatediff, rowid_encounter 
from #temp1
) a
order by rowid_encounter;
				select * from #temp3 where unique_patient='7598 - 100551' order by rowid_encounter; /* multiple same day rx */
				select * from #temp3 where unique_patient='7598 - 1090615' order by rowid_encounter;

			/* check new table */
			select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
			from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from #temp3) a,
				 (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #wells_opioid_cumul_with_strata_percent) b;
			/*
			new_ct	old_ct	ct_delta	new_mems	old_mems	mem_delta
			57310	57310	0	20310	20310	0
			*/

/* merge tables */
IF OBJECT_ID('tempdb.dbo.#temp4', 'U') IS NOT NULL DROP TABLE #temp4;
select a.*, lag_filldate, current_filldate, filldatediff, cumul_filldatediff
into #temp4
from #wells_opioid_cumul_with_strata_percent a
inner join
     #temp3 b
on a.unique_patient=b.unique_patient and
   a.rowid_encounter=b.rowid_encounter;

   			/* check new table */
			select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
			from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from #temp4) a,
				 (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #wells_opioid_cumul_with_strata_percent) b;
			/*
			new_ct	old_ct	ct_delta	new_mems	old_mems	mem_delta
			37000	57310	-20310			9219	20310		-11091
			*/
/* end */

/* put into permanent table */
		drop table [DSDWDev].dbo.AW_athena_opioids_FINAL_july13;
	select *
	into [DSDWDev].dbo.AW_athena_opioids_FINAL_july13
	from #temp4;
			create index athena_mems_idxU on [DSDWDev].dbo.AW_athena_opioids_FINAL_july13(unique_patient);

			select top 100 * from [DSDWDev].dbo.AW_athena_opioids_FINAL_july13;

			/* check copied table */
			select new_ct, old_ct, new_ct-old_ct as ct_delta, new_mems, old_mems, new_mems-old_mems as mem_delta
			from (select count(*) as new_ct, count(distinct unique_patient) as new_mems from [DSDWDev].dbo.AW_athena_opioids_FINAL_july13) a,
				 (select count(*) as old_ct, count(distinct unique_patient) as old_mems from #temp3) b;
/* end step 6 */

use DSDWDev;
/******************************************************************************************************************/
