// definitions/generate_predictions.js

// Définir les indices sur lesquels nous voulons itérer
const indices = ["DJI","cac40","Nasdaq","Nasdaq100","SP500"];

// Définir le nombre de paires de prédictions (y_proba_n, y_pred_n)
const numPredictions = 10;

// Boucler sur chaque indice
indices.forEach(index => {
  // Boucler sur chaque paire de prédictions (de 1 à 10)
  for (let i = 1; i <= numPredictions; i++) {
    // Définir le nom de la table pour cette combinaison
    const tableName = `results_app_${index}_pred_${i}`;

    // Générer le SQLX pour chaque table
    publish(tableName,{
      type: "table", // <-- AJOUTEZ CETTE LIGNE ICI
      description: `Table de résultats ${i} pour l'indice ${index}.` // Optionnel : ajoute une description dynamique
    }).query(ctx => `
      WITH params AS (
        -- Calculer la date maximale une seule fois
        SELECT MAX(DATE(Date)) AS max_date
        FROM \`financial-data-storage.prevision_prod.results_agg_${index}_pred_${i}\`
      ),
      jours_ouvres AS (
        -- Générer directement les n derniers jours ouvrés
        SELECT date_day
        FROM UNNEST(GENERATE_DATE_ARRAY(
          DATE_SUB((SELECT max_date FROM params), INTERVAL 100 DAY), 
          (SELECT max_date FROM params)
        )) AS date_day
        WHERE EXTRACT(DAYOFWEEK FROM date_day) NOT IN (1, 7) -- Exclure samedi et dimanche
        ORDER BY date_day DESC
        LIMIT 5 --  On récupere les n derniers jours ouvrés

      ),
      indicateur_achat_vente AS (
        -- Calculer les indicateurs en une seule passe
        SELECT 
          DATE(a.Date) AS Date,
          b.Ouverture,
          b.Cloture,
          a.y_proba_${i} AS proba,
          CASE 
            WHEN a.y_pred_${i} = 'O' THEN 'Neutre'
            WHEN a.y_pred_${i} = 'H' THEN 'Vente'
            WHEN a.y_pred_${i} = 'L' THEN 'Achat'
          END AS signal,
          (4 - a.step) AS Delta
        FROM \`financial-data-storage.prevision_prod.results_agg_${index}_pred_${i}\` a
        INNER JOIN \`financial-data-storage.clean_data_prod.${index}\` b 
          ON DATE(a.Date) = b.Date
        WHERE DATE(a.Date) <= (SELECT max_date FROM params)
      )
      SELECT iav.*
      FROM indicateur_achat_vente iav
      INNER JOIN jours_ouvres j 
        ON iav.Date = j.date_day
      --where iav.Delta = 0
      ORDER BY iav.Date desc
    `);
  }
});
