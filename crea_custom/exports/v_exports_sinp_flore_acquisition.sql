CREATE OR REPLACE VIEW gn_exports.v_exports_sinp_flore_acquisition AS
WITH
	af_actors AS (
		SELECT
			cafa.id_acquisition_framework,
			cafa.id_organism,
			tn.cd_nomenclature
		FROM
			gn_meta.cor_acquisition_framework_actor cafa
			LEFT JOIN ref_nomenclatures.t_nomenclatures tn ON cafa.id_nomenclature_actor_role = tn.id_nomenclature
	)
SELECT
	taf.id_acquisition_framework AS id_ca,
	taf.unique_acquisition_framework_id AS id_sinp_ca,
	taf.acquisition_framework_name AS libelle,
	NULL::TEXT AS cadre_acquisition_parent,
	taf.acquisition_framework_desc AS description,
	taf.keywords AS mots_cles,
	ref1.label_default as niveau_territorial,
	taf.territory_desc AS description_territoire,
	ref2.label_default AS objectifs_cadre_acquisition,
	ref4.label_default AS volet_SINP,
	ref3.label_default AS type_financement,
	taf.target_description AS description_cible,
	to_char(taf.meta_create_date, 'DD-MM-YYYY')::date AS date_debut,
	NULL::date AS date_fin,
	ref5.nom_organisme AS acteur1_organisme,
	NULL::TEXT AS acteur1_personne_groupe,
	'Contact principal' AS acteur1_type_role,
	ref6.nom_organisme AS acteur2_organisme,
	NULL::TEXT AS acteur2_personne_groupe,
	CASE
		WHEN ref6.nom_organisme IS NOT NULL THEN 'Producteur du jeu de données'
		ELSE NULL
	END AS acteur2_type_role,
	NULL::TEXT AS acteur3_organisme,
	NULL::TEXT AS acteur3_personne_groupe,
	NULL::TEXT AS acteur3_type_role
FROM
	gn_meta.t_acquisition_frameworks taf
	JOIN af_actors ON af_actors.id_acquisition_framework = taf.id_acquisition_framework
	JOIN gn_meta.cor_acquisition_framework_objectif obj ON obj.id_acquisition_framework = taf.id_acquisition_framework
	JOIN ref_nomenclatures.t_nomenclatures ref1 ON ref1.id_nomenclature = taf.id_nomenclature_territorial_level
	JOIN ref_nomenclatures.t_nomenclatures ref2 ON ref2.id_nomenclature = obj.id_nomenclature_objectif
	JOIN ref_nomenclatures.t_nomenclatures ref3 ON ref3.id_nomenclature = taf.id_nomenclature_financing_type
	LEFT JOIN gn_meta.cor_acquisition_framework_voletsinp sinp ON sinp.id_acquisition_framework = taf.id_acquisition_framework
	LEFT JOIN ref_nomenclatures.t_nomenclatures ref4 ON ref4.id_nomenclature = sinp.id_nomenclature_voletsinp
	LEFT JOIN utilisateurs.bib_organismes ref5 ON af_actors.id_organism = ref5.id_organisme
	AND af_actors.cd_nomenclature::INT = 1
	LEFT JOIN utilisateurs.bib_organismes ref6 ON af_actors.id_organism = ref6.id_organisme
	AND af_actors.cd_nomenclature::INT = 6
GROUP BY
	taf.id_acquisition_framework,
	taf.unique_acquisition_framework_id,
	taf.acquisition_framework_name,
	taf.acquisition_framework_desc,
	taf.keywords,
	ref1.label_default,
	taf.territory_desc,
	ref2.label_default,
	ref4.label_default,
	ref3.label_default,
	taf.target_description,
	to_char(taf.meta_create_date, 'DD-MM-YYYY')::date,
	ref5.nom_organisme,
	ref6.nom_organisme
