using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MqttZero.Tests
{
    public sealed class SourceConventionTests
    {
        [Fact]
        public void SourceFilesFollowTeamConventions()
        {
            string repositoryRoot = FindRepositoryRoot();
            string[] sourceFiles = Directory.EnumerateFiles(
                    repositoryRoot,
                    "*.cs",
                    SearchOption.AllDirectories)
                .Where(static path => !IsGeneratedPath(path))
                .ToArray();
            List<string> violations = [];

            foreach (string path in sourceFiles)
            {
                string text = File.ReadAllText(path);
                CheckLineEndings(path, text, violations);

                SyntaxTree tree = CSharpSyntaxTree.ParseText(
                    text,
                    path: path,
                    cancellationToken: TestContext.Current.CancellationToken);
                CompilationUnitSyntax root = tree.GetCompilationUnitRoot(
                    TestContext.Current.CancellationToken);
                IEnumerable<BaseTypeDeclarationSyntax> topLevelTypes = root
                    .DescendantNodes()
                    .OfType<BaseTypeDeclarationSyntax>()
                    .Where(static type => type.Parent is CompilationUnitSyntax or BaseNamespaceDeclarationSyntax);
                if (topLevelTypes.Count() > 1)
                {
                    violations.Add($"{path}: contains more than one top-level type.");
                }

                CheckBodies(path, root, violations);
            }

            Assert.True(
                violations.Count == 0,
                string.Join(Environment.NewLine, violations));
        }

        private static void CheckLineEndings(string path, string text, List<string> violations)
        {
            for (int index = 0; index < text.Length; index++)
            {
                if (text[index] == '\r')
                {
                    if (index + 1 >= text.Length || text[index + 1] != '\n')
                    {
                        violations.Add($"{path}: contains a CR without LF.");
                        return;
                    }

                    index++;
                    continue;
                }

                if (text[index] == '\n')
                {
                    violations.Add($"{path}: contains an LF without CR.");
                    return;
                }
            }
        }

        private static void CheckBodies(
            string path,
            CompilationUnitSyntax root,
            List<string> violations)
        {
            foreach (MethodDeclarationSyntax method in root.DescendantNodes().OfType<MethodDeclarationSyntax>())
            {
                if (method.ExpressionBody != null)
                {
                    violations.Add($"{path}: method '{method.Identifier.ValueText}' uses an expression body.");
                }
            }

            foreach (IfStatementSyntax statement in root.DescendantNodes().OfType<IfStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: an if statement does not use braces.");
                }
            }

            foreach (ForStatementSyntax statement in root.DescendantNodes().OfType<ForStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a for statement does not use braces.");
                }
            }

            foreach (ForEachStatementSyntax statement in root.DescendantNodes().OfType<ForEachStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a foreach statement does not use braces.");
                }
            }

            foreach (WhileStatementSyntax statement in root.DescendantNodes().OfType<WhileStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a while statement does not use braces.");
                }
            }
        }

        private static bool IsGeneratedPath(string path)
        {
            string normalized = path.Replace('\\', '/');
            return normalized.Contains("/obj/", StringComparison.Ordinal) ||
                normalized.Contains("/bin/", StringComparison.Ordinal);
        }

        private static string FindRepositoryRoot()
        {
            DirectoryInfo? directory = new(AppContext.BaseDirectory);
            while (directory != null)
            {
                if (File.Exists(Path.Combine(directory.FullName, "MqttZero.slnx")))
                {
                    return directory.FullName;
                }

                directory = directory.Parent;
            }

            throw new InvalidOperationException("Could not locate the MqttZero repository root.");
        }
    }
}
