Here is a professional, high-quality README.md in English, tailored for a seismic continuous dataset project. It follows standard GitHub repository conventions to make it look "official" and researcher-friendly.SeisStream: A Continuous Seismic Benchmark Dataset📖 OverviewSeisStream is a standardized benchmark dataset featuring high-quality continuous seismic waveforms. Unlike traditional earthquake-cut "snippets," this dataset provides long-duration continuous streams designed to evaluate the performance of automated monitoring systems, phase pickers, and event detection algorithms in real-world scenarios.Why SeisStream?Continuous Context: Preserves long-term noise characteristics and overlapping events.Benchmarking: Provides a unified "ground truth" for comparing different algorithms (e.g., PhaseNet, EQTransformer, or traditional STA/LTA).Diversity: Includes data from various tectonic settings and network geometries.🛠 FeaturesFormat: Standardized .mseed (MiniSEED) and .h5 (HDF5) formats for seamless integration with ObsPy and Deep Learning frameworks.Labels: High-precision manual catalogs including P/S-wave arrivals, polarity, and magnitude.Metadata: Comprehensive station inventory (StationXML and .csv).Pre-processed: Instrument response removed, synchronized sampling rates (100 Hz), and gap-filled.📂 Repository StructurePlaintext.
├── data/
│   ├── waveforms/        # Continuous daily/hourly mseed files
│   ├── metadata/         # Station coordinates and instrument info
│   └── catalog/          # Ground truth event labels (QuakeML/CSV)
├── scripts/
│   ├── data_loader.py    # Example Python utility to load data
│   └── evaluation.py     # Standard metrics (Precision, Recall, F1)
├── examples/
│   └── tutorial.ipynb    # Jupyter notebook for quick visualization
└── README.md
🚀 Quick Start1. InstallationEnsure you have ObsPy and NumPy installed:Bashpip install obspy numpy pandas matplotlib
2. Basic UsagePythonimport obspy
from glob import glob

# Load a day of continuous data
stream = obspy.read("data/waveforms/2026.083.*.mseed")

# Apply basic processing
stream.detrend("linear")
stream.filter("bandpass", freqmin=1.0, freqmax=45.0)

# Plot for inspection
stream.plot()
📊 Dataset StatisticsAttributeDetailsDuration30 Continuous DaysSampling Rate100 HzNum. of Stations20 (Broadband & Short-period)Total Events450+ Manual AnnotationsTarget TasksPicking, Detection, Association, Denoising📜 CitationIf you use this dataset in your research, please cite the following:Code snippet@software{seisstream_2026,
  author = {Ziye Yu},
  title = {SeisStream: A Benchmark Dataset for Continuous Seismic Waveform Analysis},
  year = {2026},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/your-username/seis-stream}}
}
🤝 ContributingContributions are welcome! If you have additional continuous data or improved labels you'd like to share, please open an Issue or submit a Pull Request.
