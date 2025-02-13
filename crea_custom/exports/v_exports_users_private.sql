CREATE OR REPLACE VIEW gn_exports.v_exports_users_private AS
SELECT
    tr.id_role,
    tr.identifiant,
    tr.nom_role,
    tr.prenom_role,
    tr.email AS email,
    tr.id_organisme,
    tr.champs_addi ->> 'ambassador' :: text AS ambassador,
    tr.champs_addi ->> 'bio' :: text AS bio,
    tr.champs_addi ->> 'birthdate' :: text AS birthdate,
    tr.champs_addi ->> 'category' :: text AS category_id,
    ref_nomenclatures.get_nomenclature_label(
        (tr.champs_addi ->> 'category' :: text) :: integer
    ) AS category,
    tr.champs_addi ->> 'city' :: text AS city,
    tr.champs_addi ->> 'country' :: text AS country,
    tr.champs_addi ->> 'gender' :: text AS gender,
    tr.champs_addi ->> 'godfather' :: text AS godfather,
    tr.champs_addi ->> 'other_ps_programs' :: text AS other_ps_programs,
    tr.champs_addi ->> 'postal_code' :: text AS postal_code,
    tr.champs_addi ->> 'profession' :: text AS profession,
    tr.champs_addi ->> 'relay' :: text AS relay,
    tr.champs_addi ->> 'starred_spots,' :: text AS starred_spots,
    tr.date_insert,
    tr.date_update
FROM
    utilisateurs.t_roles tr
WHERE
    tr.groupe IS FALSE;
