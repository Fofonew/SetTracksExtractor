import asyncio
from typing import Optional, Dict
from shazamio import Shazam

async def shazam_recognize_file(shazam: Shazam, path: str, retries: int = 3, backoff: float = 1.5) -> Optional[Dict]:
    for i in range(retries):
        try:
            # ⚠️ Avec shazamio, c'est recognize_song et non recognize
            return await shazam.recognize(path)
        except Exception as e:
            print(f"Erreur tentative {i+1}: {e}")
            if i == retries - 1:
                return None
            await asyncio.sleep(backoff ** i)
    return None

async def main():
    shazam = Shazam()
    # Mets ici le chemin complet vers ton extrait audio
    path = "./segments/part_000008.mp3"
    result = await shazam_recognize_file(shazam, path)
    if result:
        print("Résultat brut :", result)
        track = result.get("track", {})
        print(f"Titre : {track.get('title')}")
        print(f"Artiste : {track.get('subtitle')}")
    else:
        print("Aucun match trouvé.")

if __name__ == "__main__":
    asyncio.run(main())