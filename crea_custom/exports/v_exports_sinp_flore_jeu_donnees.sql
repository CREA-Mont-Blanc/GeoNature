CREATE OR REPLACE VIEW gn_exports.v_exports_sinp_flore_jeu_donnees AS
WITH
	ds_actors AS (
		SELECT
			cda.id_dataset,
			cda.id_organism,
			borg.nom_organisme,
			tn.cd_nomenclature,
			borg.uuid_organisme
		FROM
			gn_meta.cor_dataset_actor cda
			LEFT JOIN utilisateurs.bib_organismes borg ON cda.id_organism = borg.id_organisme
			LEFT JOIN ref_nomenclatures.t_nomenclatures tn ON cda.id_nomenclature_actor_role = tn.id_nomenclature
	)
SELECT
	tds.id_dataset AS id_jdd,
	tds.unique_dataset_id AS id_sinp_jdd,
	acq.acquisition_framework_name AS cadre_acquisiton,
	tds.dataset_name AS nom_jeu_donnees,
	tds.dataset_shortname AS nom_court,
	tds.dataset_desc AS description,
	ndso.label_default AS objectif,
	ncm.label_default AS methode_collecte,
	ndo.label_default AS origine_donnees,
	nss.label_default AS statut_source,
	tds.keywords AS mots_cles,
	act1.nom_organisme AS acteur1_organisme,
	NULL::TEXT AS acteur1_personne_groupe,
	'Contact principal' AS acteur1_type_role,
	act2.nom_organisme AS acteur2_organisme,
	NULL::TEXT AS acteur2_personne_groupe,
	CASE
		WHEN act2.nom_organisme IS NOT NULL THEN 'Producteur du jeu de données'
		ELSE NULL
	END AS acteur2_type_role,
	act3.nom_organisme AS acteur3_organisme,
	NULL::TEXT AS acteur3_personne_groupe,
	CASE
		WHEN act3.nom_organisme IS NOT NULL THEN 'Point de contact base de données de production'
		ELSE NULL
	END AS acteur3_type_role,
	act4.nom_organisme AS acteur4_organisme,
	NULL::TEXT AS acteur4_personne_groupe,
	CASE
		WHEN act4.nom_organisme IS NOT NULL THEN 'Point de contact pour les métadonnées'
		ELSE NULL
	END AS acteur4_type_role
FROM
	gn_meta.t_datasets tds
	JOIN ds_actors ON ds_actors.id_dataset = tds.id_dataset
	JOIN gn_meta.t_acquisition_frameworks acq ON tds.id_acquisition_framework = acq.id_acquisition_framework
	LEFT JOIN gn_meta.cor_dataset_territory cdt ON cdt.id_dataset = tds.id_dataset
	LEFT JOIN ref_nomenclatures.t_nomenclatures ndt ON tds.id_nomenclature_data_type = ndt.id_nomenclature
	LEFT JOIN ref_nomenclatures.t_nomenclatures ncm ON tds.id_nomenclature_collecting_method = ncm.id_nomenclature
	LEFT JOIN ref_nomenclatures.t_nomenclatures ndo ON tds.id_nomenclature_data_origin = ndo.id_nomenclature
	LEFT JOIN ref_nomenclatures.t_nomenclatures ndso ON tds.id_nomenclature_dataset_objectif = ndso.id_nomenclature
	LEFT JOIN ref_nomenclatures.t_nomenclatures nrt ON tds.id_nomenclature_resource_type = nrt.id_nomenclature
	LEFT JOIN ref_nomenclatures.t_nomenclatures nss ON tds.id_nomenclature_source_status = nss.id_nomenclature
	LEFT JOIN utilisateurs.bib_organismes act1 ON ds_actors.id_organism = act1.id_organisme
	AND ds_actors.cd_nomenclature::INT = 1
	LEFT JOIN utilisateurs.bib_organismes act2 ON ds_actors.id_organism = act2.id_organisme
	AND ds_actors.cd_nomenclature::INT = 6
	LEFT JOIN utilisateurs.bib_organismes act3 ON ds_actors.id_organism = act3.id_organisme
	AND ds_actors.cd_nomenclature::INT = 7
	LEFT JOIN utilisateurs.bib_organismes act4 ON ds_actors.id_organism = act4.id_organisme
	AND ds_actors.cd_nomenclature::INT = 8
GROUP BY
	tds.id_dataset,
	acq.acquisition_framework_name,
	tds.unique_dataset_id,
	tds.dataset_name,
	tds.dataset_shortname,
	tds.dataset_desc,
	ndso.label_default,
	ncm.label_default,
	ndo.label_default,
	nss.label_default,
	ds_actors.nom_organisme,
	act1.nom_organisme,
	act2.nom_organisme,
	act3.nom_organisme,
	act4.nom_organisme
