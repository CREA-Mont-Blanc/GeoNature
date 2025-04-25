CREATE OR REPLACE VIEW gn_exports.v_exports_sinp_flore_observations AS
WITH
       af_actors AS (
              SELECT
                     cafa.id_acquisition_framework
              FROM
                     gn_meta.cor_acquisition_framework_actor cafa
                     LEFT JOIN utilisateurs.bib_organismes borg ON cafa.id_organism = borg.id_organisme
       ),
       af AS (
              SELECT
                     taf.id_acquisition_framework
              FROM
                     gn_meta.t_acquisition_frameworks taf
                     JOIN af_actors ON af_actors.id_acquisition_framework = taf.id_acquisition_framework
              GROUP BY
                     taf.id_acquisition_framework
       ),
       ds AS (
              SELECT
                     tds.id_dataset,
                     tds.dataset_name AS nom_jdd,
                     tds.id_acquisition_framework
              FROM
                     gn_meta.t_datasets tds
              GROUP BY
                     tds.id_dataset,
                     tds.dataset_name
       ),
       geo AS (
              SELECT
                     s_1.id_synthese,
                     "left" (geo_1.area_code::TEXT, 2) AS departement,
                     geo_1.area_code AS commune
              FROM
                     ref_geo.l_areas geo_1
                     JOIN gn_synthese.synthese s_1 ON st_intersects (
                            s_1.the_geom_4326,
                            st_transform (geo_1.geom, 4326)
                     )
              WHERE
                     geo_1.id_type = 25
       )
SELECT
       ds.nom_jdd AS nom_jdd,
       s.unique_id_sinp_grp AS id_sinp_releve,
       occ.id_releve_occtax AS identifiant_releve,
       NULL::TEXT AS code_perso_releve,
       s.unique_id_sinp AS id_sinp_observation,
       s.id_synthese AS identifiant_observation,
       geo.departement,
       geo.commune,
       NULL::TEXT AS lieu_dit,
       CASE
              WHEN "position" (sp.srtext::TEXT, 'GEOGCS'::TEXT) = 1 THEN "substring" (
                     replace(sp.srtext::TEXT, 'GEOGCS["'::TEXT, ''::TEXT),
                     1,
                     "position" (
                            replace(sp.srtext::TEXT, 'GEOGCS["'::TEXT, ''::TEXT),
                            '",'::TEXT
                     ) - 1
              )
              WHEN "position" (sp.srtext::TEXT, 'PROJCS'::TEXT) = 1 THEN "substring" (
                     replace(sp.srtext::TEXT, 'PROJCS["'::TEXT, ''::TEXT),
                     1,
                     "position" (
                            replace(sp.srtext::TEXT, 'PROJCS["'::TEXT, ''::TEXT),
                            '",'::TEXT
                     ) - 1
              )
              WHEN "position" (sp.srtext::TEXT, 'GEOCCS'::TEXT) = 1 THEN "substring" (
                     replace(sp.srtext::TEXT, 'GEOCCS["'::TEXT, ''::TEXT),
                     1,
                     "position" (
                            replace(sp.srtext::TEXT, 'GEOCCS["'::TEXT, ''::TEXT),
                            '",'::TEXT
                     ) - 1
              )
              WHEN "position" (sp.srtext::TEXT, 'COMPD_CS'::TEXT) = 1 THEN "substring" (
                     replace(sp.srtext::TEXT, 'COMPD_CS["'::TEXT, ''::TEXT),
                     1,
                     "position" (
                            replace(sp.srtext::TEXT, 'COMPD_CS["'::TEXT, ''::TEXT),
                            '",'::TEXT
                     ) - 1
              )
              ELSE 'Non défini'::TEXT
       END AS sys_coord,
       st_srid (s.the_geom_4326) AS srid,
       st_astext (s.the_geom_4326) AS localisation_wkt,
       st_x (st_centroid (s.the_geom_4326)) AS coord_x,
       st_y (st_centroid (s.the_geom_4326)) AS coord_y,
       s."precision" AS PRECISION,
       NULL::TEXT AS nature_objet,
       s.altitude_min AS alti_min,
       s.altitude_max AS alti_max,
       NULL::TEXT AS pente,
       NULL::TEXT AS exposition,
       NULL::TEXT AS comm_geol,
       NULL::TEXT AS milieu,
       s.observers AS observateurs,
       s.date_min AS date_debut,
       s.date_max AS date_fin,
       NULL::TEXT AS comm_context,
       n2.mnemonique AS type_regroupement,
       s.grp_method AS meth_regroupement,
       NULL::TEXT AS surface,
       NULL::TEXT AS strate_vegetation,
       NULL::TEXT AS hauteur_strate,
       NULL::TEXT AS recouvrement_strate,
       s.cd_hab AS cdhab,
       NULL::TEXT AS cdhab_v,
       NULL::TEXT AS code_eur,
       NULL::TEXT AS code_eunis,
       NULL::TEXT AS code_cahab,
       NULL::TEXT AS code_cb,
       NULL::TEXT AS id_microhab,
       n21.regne AS regne,
       s.nom_cite AS nom_cite,
       s.cd_nom as cd_nom,
       NULL::TEXT AS abondance,
       NULL::TEXT AS sociabilite,
       n11.mnemonique AS sexe,
       n7.mnemonique AS naturalite,
       NULL::TEXT AS comm_description,
       n15.mnemonique AS statut_observation,
       n12.mnemonique AS objet_denombrement,
       n13.mnemonique AS type_denombrement,
       s.count_min AS nombre_min,
       s.count_max AS nombre_max,
       n17.label_default AS statut_source,
       NULL::TEXT AS reference_biblio,
       NULL::TEXT AS page,
       n8.mnemonique AS preuve_existence,
       s.digital_proof AS preuve_numerique,
       s.non_digital_proof AS preuve_non_numerique,
       NULL::TEXT AS nom_collection,
       NULL::TEXT AS ref_collection,
       s.determiner AS determinateur,
       NULL::TEXT AS niv_val,
       NULL::TEXT AS niveau_diffusion,
       n16.mnemonique AS floutage_dee,
       NULL::TEXT AS methode_observation,
       n6.mnemonique AS etat_biologique,
       n5.mnemonique AS statut_biologique,
       n10.mnemonique AS stade_vie,
       n19.mnemonique AS methode_determination,
       n3.mnemonique AS comportement
FROM
       gn_synthese.synthese s
       JOIN ds ON ds.id_dataset = s.id_dataset
       JOIN af ON ds.id_acquisition_framework = af.id_acquisition_framework
       JOIN geo ON s.id_synthese = geo.id_synthese
       JOIN spatial_ref_sys sp ON st_srid (s.the_geom_4326) = sp.auth_srid
       LEFT JOIN ref_habitats.habref h ON h.cd_hab = s.cd_hab
       LEFT JOIN pr_occtax.t_releves_occtax occ ON occ.unique_id_sinp_grp = s.unique_id_sinp_grp
       LEFT JOIN ref_nomenclatures.t_nomenclatures n1 ON s.id_nomenclature_geo_object_nature = n1.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n2 ON s.id_nomenclature_grp_typ = n2.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n3 ON s.id_nomenclature_behaviour = n3.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n4 ON s.id_nomenclature_obs_technique = n4.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n5 ON s.id_nomenclature_bio_status = n5.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n6 ON s.id_nomenclature_bio_condition = n6.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n7 ON s.id_nomenclature_naturalness = n7.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n8 ON s.id_nomenclature_exist_proof = n8.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n9 ON s.id_nomenclature_diffusion_level = n9.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n10 ON s.id_nomenclature_life_stage = n10.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n11 ON s.id_nomenclature_sex = n11.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n12 ON s.id_nomenclature_obj_count = n12.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n13 ON s.id_nomenclature_type_count = n13.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n14 ON s.id_nomenclature_sensitivity = n14.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n15 ON s.id_nomenclature_observation_status = n15.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n16 ON s.id_nomenclature_blurring = n16.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n17 ON s.id_nomenclature_source_status = n17.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n18 ON s.id_nomenclature_info_geo_type = n18.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n19 ON s.id_nomenclature_determination_method = n19.id_nomenclature
       LEFT JOIN ref_nomenclatures.t_nomenclatures n20 ON s.id_nomenclature_valid_status = n20.id_nomenclature
       LEFT JOIN taxonomie.taxref n21 ON s.cd_nom = n21.cd_nom
       JOIN gn_synthese.cor_area_synthese cas ON cas.id_synthese = s.id_synthese
WHERE
       n21.regne::TEXT = 'Plantae'::TEXT
       AND cas.id_area = 34946
