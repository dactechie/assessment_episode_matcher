

import pandas as pd
# import numpy as np
# from rapidfuzz.distance import Levenshtein
from rapidfuzz import fuzz

# def calculate_similarity(slk1, slk2):
#     distance = Levenshtein.distance(slk1, slk2)
#     max_len = max(len(slk1), len(slk2))
#     return 1 - (distance / max_len)

def calculate_similarity(slk1, slk2):
    # Using ratio method for similarity score
    return fuzz.ratio(slk1, slk2) / 100

def find_nearest_matches(unmatched_slks, database_slks, threshold=0.5):
    results = []
    
    for u_slk in unmatched_slks:
        best_match = None
        best_similarity = -1
        
        for d_slk in database_slks:
            similarity = calculate_similarity(u_slk, d_slk)
            
            if similarity > best_similarity:
                best_match = d_slk
                best_similarity = similarity
        
        if best_similarity >= threshold:
            results.append((u_slk, best_match, best_similarity))
        else:
            results.append((u_slk, None, best_similarity))
    
    return results



# Example usage
# unmatched_slks = ["DOEJ150119902", "SMIJ020219851"]
# database_slks = ["DOEJ150119902", "JONJ150119902", "SMIJ020219851", "JONJ150119851"]
# threshold = 0.5

# matches = find_nearest_matches(unmatched_slks, database_slks, threshold)
# for match in matches:
#   print(f"Unmatched SLK: {match[0]}, Best Match: {match[1]}, Similarity: {match[2]}")

# import pandas as pd
# import difflib

# from assessment_episode_matcher.matching.main import get_closest_slk_match2

# Add the project root directory to the Python module search path

    # # if config and config.get("get_nearest_slk",0) == 1:
    # slk_onlyin_ep['closest_atom_SLK'] = get_closest_slk_match(           
    #         a_ineprogs.SLK.unique().tolist()
    #         , slk_onlyin_ep.SLK
    #         )

# def test_get_closest_slk_match ():
#   not_matched_SLKs = ["XLLFT21071981"] #atoms_df
#   slk_onlyin_ep =  pd.Series (["ALLFT210719811"])

#   # for each item in the series  slk_onlyin_ep
#   # find the closest match in the list not_matched_SLKs 
#   result:pd.Series  = get_closest_slk_match(not_matched_SLKs
#                         ,slk_onlyin_ep, match_threshold=0.7)
#   assert result.to_list() == slk_onlyin_ep.to_list()


# def test_get_closest_slk_match3 (not_matched:list[str], try_match:list[str]):
#     results = []
#     for nm in not_matched:
#       closest_match = difflib.get_close_matches(word=nm
#                                               , possibilities=try_match
#                                               , n=1, cutoff=0.75)
#       results.append(closest_match[0] if closest_match else None)
#     return results

# def test_get_closest_slk_match2 ():
#   not_matched_SLKs =  pd.Series ( [ "XLLFT21071981"]) #atoms_df
#   slk_onlyin_ep = ["B","ALLFT210719811","C"]

#   # for each item in the series  slk_onlyin_ep
#   # find the closest match in the list not_matched_SLKs 
#   result:pd.Series  = get_closest_slk_match2(not_matched_SLKs
#                         ,slk_onlyin_ep)
#   assert result.to_list() == slk_onlyin_ep


# def split_word(word):
#     half = len(word) // 2
#     return word[:half], word[half:]

# # Checks for at least 50% match for either half
# def is_potential_match(first_half, second_half, candidate, threshold=0.5):
#     return difflib.SequenceMatcher(None, first_half, candidate).ratio() >= threshold \
#             or difflib.SequenceMatcher(None, second_half, candidate).ratio() >= threshold

# def get_candidates_pandas(not_matched:list[str], try_match:list[str]):
#     # Create DataFrames
#     df_not_matched = pd.DataFrame(not_matched, columns=['word'])
#     df_try_match = pd.DataFrame(try_match, columns=['candidate'])

#     # Split words into halves
#     df_not_matched[['first_half', 'second_half']] = df_not_matched['word'].apply(
#         lambda x: pd.Series(split_word(x))
#     )

#     # Initialize candidates dictionary
#     candidates_dict = {}

#     # Check for 50% match for either half
#     for nm in df_not_matched.itertuples():
#         candidates = df_try_match['candidate'].apply(
#             lambda x: is_potential_match(nm.first_half, nm.second_half, x, threshold=0.5)
#             # (difflib.SequenceMatcher(None, nm.first_half, x).ratio() >= 0.5) or
#             #           (difflib.SequenceMatcher(None, nm.second_half, x).ratio() >= 0.5)
#         )
#         matched_candidates = df_try_match[candidates]['candidate'].tolist()
#         candidates_dict[nm.word] = matched_candidates

#     return candidates_dict
# # def get_closest_slk_match_optimized(not_matched, try_match):
# #     # Splits a word into two halves
# #     def split_word(word):
# #         half = len(word) // 2
# #         return word[:half], word[half:]

# #     result = {}
# #     for nm in not_matched:
# #         first_half, second_half = split_word(nm)
# #         candidates = set()
        
# #         for tm in try_match:
# #             if (difflib.SequenceMatcher(None, first_half, tm).ratio() >= 0.5 or 
# #                 difflib.SequenceMatcher(None, second_half, tm).ratio() >= 0.5):
# #                 candidates.add(tm)
        
# #         result[nm] = list(candidates)
    
# #     return result

# def test_get_closest_slk_match3(not_matched:list[str], try_match:list[str]):
#     # Get candidate lists using the optimized function
#   candidate_lists = get_candidates_pandas(not_matched, try_match)
    
#   closest_matches = {}
#   for nm, candidates in candidate_lists.items():
#       if candidates:
#           closest_match = difflib.get_close_matches(word=nm, possibilities=candidates, n=1, cutoff=0.75)
#           closest_matches[nm] = closest_match[0] if closest_match else None
#       # else:
#       #     closest_matches[nm] = None
  
#   return closest_matches

def setup():
    slk_onlyinass = pd.DataFrame({
        'SLK': [ "BBBZZ121220129","CCCDD301020232", "XLLFT21071981","DD", "CCCDD301020232"],
        'AssessmentDate': [pd.Timestamp('2022-01-01')
                           , pd.Timestamp('2022-01-02')
                           , pd.Timestamp('2022-01-03')
                           , pd.Timestamp('2022-01-04'), pd.Timestamp('2022-01-05')]
    })
    slk_onlyin_ep = pd.DataFrame({
        'SLK': ["B","ALLFT210719811","C"]   
    })    
    # a_ineprogs_slk = pd.Series(a_ineprogs.SLK.unique()).tolist()
    # a_ineprogs_slk = a_ineprogs.SLK.unique().tolist()
    # slk_onlyin_ep_slk = slk_onlyin_ep.SLK.unique().tolist()

    return slk_onlyinass, slk_onlyin_ep

def main():
 slk_onlyinass, slk_onlyin_ep = setup()
 slk_onlyinass_slk = slk_onlyinass.SLK.unique().tolist()
 slk_onlyin_ep_slk = slk_onlyin_ep.SLK.unique().tolist()
 matches = find_nearest_matches(slk_onlyinass_slk, slk_onlyin_ep_slk, threshold=0.5)    
 for match in matches:
   print(f"Unmatched SLK: {match[0]}, Best Match: {match[1]}, Similarity: {match[2]}")

def test_find_nearest_matches():
    main()

#     #     slk_onlyin_ep['closest_atom_SLK'] = nearest_to_atom_slk_from_ep.reindex(slk_onlyin_ep.index)
#     # slk_onlyin_ep['closest_atom_SLK'] = nearest_to_atom_slk_from_ep.reindex(slk_onlyin_ep.index)    


# def main():
#     slk_onlyinass, slk_onlyin_ep = setup()
#     slk_onlyinass_slk = slk_onlyinass.SLK.unique().tolist()
#     slk_onlyin_ep_slk = slk_onlyin_ep.SLK.unique().tolist()

#     nearest_to_atom_slk_from_ep = test_get_closest_slk_match3(           
#           slk_onlyinass_slk
#           , slk_onlyin_ep_slk
#           )
#     for not_matched_slk, nearest_match in nearest_to_atom_slk_from_ep.items():
#         slk_onlyinass.loc[slk_onlyinass.SLK == not_matched_slk, 'ClosestMatch'] = nearest_match
#         print(f"SLK: {not_matched_slk} -> Closest Match: {nearest_match}")

#     print(slk_onlyinass)

if __name__ == '__main__':
    main()
  # not_matched_SLKs =  [ "BBBB","CCCC", "XLLFT21071981","DD"]
  # slk_onlyin_ep = ["B","ALLFT210719811","C"]   
  # r = test_get_closest_slk_match3(not_matched_SLKs,slk_onlyin_ep )
  # print(r)

# def test_perform_date_matches():
#   # Test case 1: Empty input
#   assert perform_date_matches([], []) == []

#   # Test case 2: Single match
#   episodes = [
#     {'id': 1, 'title': 'Episode 1', 'date': '2022-01-01'},
#     {'id': 2, 'title': 'Episode 2', 'date': '2022-01-02'},
#     {'id': 3, 'title': 'Episode 3', 'dset PYTHONPATH=C:\Users\aftab.jalal\dev\assessment_episode_matcher;%PYTHONPATH%set PYTHONPATH=C:\Users\aftab.jalal\dev\assessment_episode_matcher;%PYTHONPATH%ate': '2022-01-03'}
#   ]
#   dates = ['2022-01-02']
#   expected_result = [{'id': 2, 'title': 'Episode 2', 'date': '2022-01-02'}]
#   assert perform_date_matches(episodes, dates) == expected_result

#   # Test case 3: Multiple matches
#   episodes = [
#     {'id': 1, 'title': 'Episode 1', 'date': '2022-01-01'},
#     {'id': 2, 'title': 'Episode 2', 'date': '2022-01-02'},
#     {'id': 3, 'title': 'Episode 3', 'date': '2022-01-02'},
#     {'id': 4, 'title': 'Episode 4', 'date': '2022-01-03'}
#   ]
#   dates = ['2022-01-02']
#   expected_result = [
#     {'id': 2, 'title': 'Episode 2', 'date': '2022-01-02'},
#     {'id': 3, 'title': 'Episode 3', 'date': '2022-01-02'}
#   ]
#   assert perform_date_matches(episodes, dates) == expected_result

#   # Test case 4: No matches
#   episodes = [
#     {'id': 1, 'title': 'Episode 1', 'date': '2022-01-01'},
#     {'id': 2, 'title': 'Episode 2', 'date': '2022-01-02'},
#     {'id': 3, 'title': 'Episode 3', 'date': '2022-01-03'}
#   ]
#   dates = ['2022-01-04']
#   expected_result = []
#   assert perform_date_matches(episodes, dates) == expected_result