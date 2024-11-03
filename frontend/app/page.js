import SQLConverter from "@/components/SQLConverter";

export default function Home() {
  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-3">
          <h1 className="text-xl font-semibold text-gray-800">
            SQL to Excel Converter
          </h1>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-4 py-8">
        <SQLConverter />
      </main>
    </div>
  );
}
