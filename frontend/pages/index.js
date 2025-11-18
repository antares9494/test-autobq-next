import { useState } from "react";

export default function Home() {
  const [file, setFile] = useState(null);
  const [comp471, setComp471] = useState("");
  const [comp512, setComp512] = useState("");
  const [preview, setPreview] = useState(null);
  const [downloadUrl, setDownloadUrl] = useState(null);
  const [loading, setLoading] = useState(false);

  const onSubmit = async (e) => {
    e.preventDefault();
    if (!file) return alert("Choisissez un PDF");
    setLoading(true);
    const fd = new FormData();
    fd.append("file", file);
    fd.append("compte471", comp471);
    fd.append("compte512", comp512);
    try {
      const res = await fetch("http://localhost:8000/process", {
        method: "POST",
        body: fd,
      });
      if (!res.ok) {
        const err = await res.text();
        throw new Error(err);
      }
      const data = await res.json();
      setPreview(data.preview || []);
      setDownloadUrl(data.download_url);
    } catch (err) {
      alert("Erreur : " + err.message);
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const onDownload = () => {
    if (!downloadUrl) return;
    window.open("http://localhost:8000" + downloadUrl, "_blank");
  };

  return (
    <main style={{ padding: 20 }}>
      <h1>Processus SG → ACD (local)</h1>
      <form onSubmit={onSubmit}>
        <div>
          <label>PDF (Société Générale) : </label>
          <input type="file" accept="application/pdf" onChange={(e) => setFile(e.target.files[0])} />
        </div>
        <div>
          <label>Compte 471 (optionnel) : </label>
          <input value={comp471} onChange={(e) => setComp471(e.target.value)} />
        </div>
        <div>
          <label>Compte 512 (optionnel) : </label>
          <input value={comp512} onChange={(e) => setComp512(e.target.value)} />
        </div>
        <div style={{ marginTop: 10 }}>
          <button type="submit" disabled={loading}>{loading ? "Traitement..." : "Lancer"}</button>
          {downloadUrl && <button type="button" onClick={onDownload} style={{ marginLeft: 10 }}>Télécharger Excel</button>}
        </div>
      </form>
      <section style={{ marginTop: 20 }}>
        <h2>Aperçu</h2>
        {preview ? (
          <pre style={{ maxHeight: 400, overflow: "auto", background: "#f7f7f7", padding: 10 }}>
            {JSON.stringify(preview, null, 2)}
          </pre>
        ) : (
          <p>Aucun aperçu</p>
        )}
      </section>
    </main>
  );
}
