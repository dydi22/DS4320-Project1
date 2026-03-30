# DS 4320 Project 1: ATP Tennis Match Prediction with a Relational Database

## Executive Summary
This repository contains my DS 4320 Project 1 on predicting ATP men’s singles match outcomes using a relational dataset built from historical tennis records. The project includes data acquisition, relational modeling, DuckDB-based analysis, a machine learning pipeline, metadata documentation, a press release, and linked project data.

**Name:** Dylan Dietrich  
**NetID:** atv7xh  
**DOI:** [your DOI link]  
**Press Release:** [press_release.md](./press_release.md)  
**Data:** [data folder](./data/)  
**Pipeline:** [pipeline notebook](./notebooks/your_notebook.ipynb)  
**License:** [MIT License](./LICENSE)

## Problem Definition

### Initial General Problem and Refined Specific Problem Statement

**Initial general problem:** Predicting sports game outcomes.

**Refined specific problem:** This project focuses on predicting ATP men’s singles match outcomes. More specifically, it estimates the probability that Player 1 defeats Player 2 in a future ATP singles match using only pre-match information available before the match date, including player ranking history, prior match performance, tournament context, and recent workload features stored in a relational database.

### Rationale for the Refinement

I refined the general problem of predicting sports game outcomes into ATP men’s singles match prediction because first of all I know a lot about tennis and I am obsessed with it. Secondly, tennis provides a well-defined one-versus-one setting, a clear target variable, and publicly available historical match data. Narrowing the problem to ATP singles makes it possible to create a clean relational dataset with linked tables for players, matches, rankings, tournaments, and player-level match facts rather than trying to combine multiple sports with different structures. This refinement improves both the clarity of the project and the quality of the secondary dataset while still addressing a meaningful real-world prediction problem.

### Motivation for the Project

The motivation for this project is that I always feel like the tennis odds for the grandslam matches are off. I am always sure that the favorites should have a stronger edge and therefore higher precentage to win the match. Another thing is, tennis predictions are often discussed using isolated statistics such as ranking, recent form, or head-to-head records, but those pieces of information are usually scattered across different sources and are not organized into one consistent analytical system. By building a relational model, this project creates a reusable foundation for both querying and prediction. The goal is not only to estimate match outcomes more systematically and accurately, but also to show how a well-designed secondary dataset can transform raw sports records into a practical decision-support tool for analysts, and coaches.

### Press Release Headline and Link

**Headline:** *New Tennis Match Prediction System Uses Rankings, Form, and Workload to Estimate ATP Match Outcomes*  
**Link:** [press_release.md](./press_release.md)

---



